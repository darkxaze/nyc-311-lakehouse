"""
glue_jobs/deploy_and_run.py

Deploys a Glue PySpark script to S3, creates/updates the Glue job
definition, starts a run, and polls until it completes. On failure,
automatically fetches the CloudWatch error log tail, since Glue's
get_job_run ErrorMessage is often unhelpful (e.g. just "SystemExit: 1").

Reads the Glue IAM role ARN from `terraform output -raw glue_role_arn`
rather than hardcoding it.

MaxRetries is explicitly set to 0 — Glue jobs have no built-in
checkpointing, so a retry re-runs the entire job from scratch and
silently re-bills DPU-hours on failure. Fail loudly once, fix, rerun
manually.

start_job_run retries a few times on ConcurrentRunsExceededException:
Glue's default MaxConcurrentRuns=1 can reject a new run for a few
seconds after the previous run reaches a terminal state while Glue
finishes releasing the slot. Benign timing issue, distinct from
MaxRetries (which governs the job itself retrying, not the start call).

Requires the GlueJobLogRead inline policy (logs:GetLogEvents,
logs:DescribeLogStreams scoped to /aws-glue/jobs/error and
/aws-glue/jobs/output) in addition to AWSGlueConsoleFullAccess,
AmazonS3FullAccess, and the GlueRoleManagement PassRole policy.

Format flags (mutually exclusive) select which Spark confs get attached
and which Glue engine version is used:
    --iceberg       Glue 4.0, glue_catalog only (raw_to_iceberg.py)
    --iceberg-delta Glue 5.0, both glue_catalog (Iceberg read) and
                     spark_catalog (Delta write) configured. Needed by
                     any job reading an Iceberg source and writing Delta
                     (iceberg_to_delta.py). Not UniForm-specific — this
                     just wires up both catalogs; UniForm's automatic
                     Iceberg conversion was evaluated and deferred (see
                     DECISIONS.md — requires a real Hive Metastore,
                     which Glue Data Catalog's Hive-compatibility layer
                     does not provide for this feature).
    --delta         Glue 5.0, spark_catalog only (compaction.py — reads
                     and writes only the already-Delta table)

Usage:
    python glue_jobs/deploy_and_run.py --job-name raw_to_iceberg \
        --script-path glue_jobs/raw_to_iceberg.py \
        --iceberg --validate-only

    python glue_jobs/deploy_and_run.py --job-name iceberg_to_delta \
        --script-path glue_jobs/iceberg_to_delta.py \
        --iceberg-delta --test

    python glue_jobs/deploy_and_run.py --job-name compaction \
        --script-path glue_jobs/compaction.py \
        --delta
"""

import argparse
import logging
import subprocess
import time

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("deploy_and_run")

AWS_REGION = "ap-south-1"
S3_BUCKET = "nyc-311-lakehouse"
POLL_INTERVAL_SECONDS = 30
POLL_TIMEOUT_SECONDS = 3600  # 1 hour ceiling

START_JOB_MAX_RETRIES = 5
START_JOB_RETRY_DELAY_SECONDS = 10

# Required for any job reading/writing Iceberg tables via Glue Catalog.
# spark.sql.extensions is required for Iceberg's auto-clustering on
# partitioned writes — missing it causes "Incoming records violate the
# writer assumption" errors on partitioned CTAS writes. Missing any of
# these confs = silent fallback to plain Parquet or a write failure.
ICEBERG_CONF = (
    "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
    f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{S3_BUCKET}/silver/"
)

# Pure Delta job (compaction.py) — no Iceberg catalog needed, table is
# already Delta. spark_catalog override is required for DeltaTable.forPath
# and OPTIMIZE/VACUUM SQL syntax to resolve.
DELTA_CONF = (
    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
)

# iceberg_to_delta.py reads via glue_catalog (Iceberg) and writes via
# spark_catalog (Delta) in the same job — both extension sets must be
# registered together, comma-joined in a single spark.sql.extensions value
# (Spark only reads the last spark.sql.extensions conf if set twice).
ICEBERG_DELTA_CONF = (
    "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,"
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions "
    "--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog "
    "--conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog "
    "--conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog "
    "--conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO "
    f"--conf spark.sql.catalog.glue_catalog.warehouse=s3://{S3_BUCKET}/silver/"
)

# glue_version per format: Delta table registration (saveAsTable) and
# dual-catalog jobs need Glue 5.0 (Spark 3.5 / Delta 3.3.0). Pure Iceberg
# jobs stay on Glue 4.0 — already validated on the full 19M-row load in
# Stage 2, no reason to move it.
FORMAT_CONFIGS = {
    "iceberg": {"datalake_formats": "iceberg", "conf": ICEBERG_CONF, "glue_version": "4.0"},
    "iceberg_delta": {"datalake_formats": "iceberg,delta", "conf": ICEBERG_DELTA_CONF, "glue_version": "5.0"},
    "delta": {"datalake_formats": "delta", "conf": DELTA_CONF, "glue_version": "5.0"},
}
DEFAULT_GLUE_VERSION = "4.0"  # used only when no format flag is given


def get_glue_role_arn() -> str:
    """Read glue_role_arn from Terraform output rather than hardcoding it."""
    try:
        result = subprocess.run(
            ["terraform", "-chdir=terraform", "output", "-raw", "glue_role_arn"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        logger.error("Failed to read glue_role_arn from Terraform output: %s", exc.stderr)
        raise RuntimeError(
            "Could not read glue_role_arn. Has `terraform apply` been run "
            "since the output was added?"
        ) from exc

    role_arn = result.stdout.strip()
    logger.info("Using Glue role: %s", role_arn)
    return role_arn


def upload_script(script_path: str, job_name: str) -> str:
    """Upload the Glue job script to S3, return its S3 path."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3_key = f"glue_jobs/{job_name}.py"
    logger.info("Uploading %s to s3://%s/%s", script_path, S3_BUCKET, s3_key)
    s3.upload_file(script_path, S3_BUCKET, s3_key)
    return f"s3://{S3_BUCKET}/{s3_key}"


def create_or_update_job(
    job_name: str, script_s3_path: str, role_arn: str, format_key: str | None, glue_version: str
) -> None:
    """Create the Glue job if it doesn't exist, otherwise update it in place."""
    glue = boto3.client("glue", region_name=AWS_REGION)

    default_arguments = {
        "--TempDir": f"s3://{S3_BUCKET}/glue_temp/",
        "--job-language": "python",
    }
    if format_key:
        config = FORMAT_CONFIGS[format_key]
        default_arguments["--datalake-formats"] = config["datalake_formats"]
        default_arguments["--conf"] = config["conf"]
        # Required for saveAsTable/CatalogTable-backed writes to resolve through
        # AWS Glue Data Catalog as the actual Hive Metastore, not an ephemeral
        # in-container one. Needed by any job that registers a Delta table.
        if format_key in ("iceberg_delta", "delta"):
            default_arguments["--enable-glue-datacatalog"] = "true"

    job_config = {
        "Role": role_arn,
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_s3_path,
            "PythonVersion": "3",
        },
        "GlueVersion": glue_version,
        "DefaultArguments": default_arguments,
        "NumberOfWorkers": 5,
        "WorkerType": "G.1X",
        "MaxRetries": 0,  # explicit — no silent re-run/re-bill on failure; see module docstring
    }

    try:
        glue.get_job(JobName=job_name)
        logger.info("Job %s exists, updating definition (Glue %s)", job_name, glue_version)
        glue.update_job(JobName=job_name, JobUpdate=job_config)
    except glue.exceptions.EntityNotFoundException:
        logger.info("Job %s does not exist, creating it (Glue %s)", job_name, glue_version)
        glue.create_job(Name=job_name, **job_config)


def start_job_with_retry(glue, job_name: str, start_kwargs: dict) -> str:
    """Call start_job_run, retrying on ConcurrentRunsExceededException.

    Transient slot-release delay from a prior run reaching a terminal
    state, not a genuine concurrency conflict — see module docstring.
    """
    for attempt in range(1, START_JOB_MAX_RETRIES + 1):
        try:
            response = glue.start_job_run(**start_kwargs)
            return response["JobRunId"]
        except glue.exceptions.ConcurrentRunsExceededException:
            if attempt == START_JOB_MAX_RETRIES:
                raise
            logger.info(
                "Concurrent runs exceeded. Retry %d/%d in %ds...",
                attempt, START_JOB_MAX_RETRIES, START_JOB_RETRY_DELAY_SECONDS,
            )
            time.sleep(START_JOB_RETRY_DELAY_SECONDS)

    raise RuntimeError("Unreachable")


def fetch_job_error_logs(run_id: str, max_lines: int = 200) -> str:
    """Fetch the tail of the Glue job's CloudWatch error log for a run.

    get_job_run's ErrorMessage is often just 'SystemExit: 1' — the real
    traceback lives in CloudWatch under /aws-glue/jobs/error, log stream
    = run_id.
    """
    logs_client = boto3.client("logs", region_name=AWS_REGION)
    log_group = "/aws-glue/jobs/error"

    try:
        response = logs_client.get_log_events(
            logGroupName=log_group,
            logStreamName=run_id,
            limit=max_lines,
            startFromHead=False,
        )
        events = response.get("events", [])
        if not events:
            return f"(no log events found in {log_group}/{run_id} — logs may take a minute to appear)"
        return "\n".join(e["message"] for e in events)
    except logs_client.exceptions.ResourceNotFoundException:
        return f"(log stream {run_id} not found in {log_group} yet)"
    except ClientError as exc:
        return f"(failed to fetch logs: {exc})"


def start_and_poll_job(job_name: str, extra_arguments: dict | None = None) -> str:
    """Start a job run and poll until it reaches a terminal state.

    On FAILED/TIMEOUT/STOPPED, fetches and prints the CloudWatch error
    log tail before raising, so the real traceback is visible without a
    manual console trip.
    """
    glue = boto3.client("glue", region_name=AWS_REGION)

    start_kwargs = {"JobName": job_name}
    if extra_arguments:
        start_kwargs["Arguments"] = extra_arguments
        logger.info("Starting job with extra arguments: %s", extra_arguments)

    run_id = start_job_with_retry(glue, job_name, start_kwargs)
    logger.info("Started job run %s (run id: %s)", job_name, run_id)

    elapsed = 0
    while elapsed < POLL_TIMEOUT_SECONDS:
        run = glue.get_job_run(JobName=job_name, RunId=run_id)
        state = run["JobRun"]["JobRunState"]
        logger.info("Job %s run %s state: %s (%ds elapsed)", job_name, run_id, state, elapsed)

        if state == "SUCCEEDED":
            logger.info("Job %s completed successfully", job_name)
            return state

        if state in ("FAILED", "TIMEOUT", "STOPPED"):
            error_message = run["JobRun"].get("ErrorMessage", "no error message provided")
            logger.error("Job failed with ErrorMessage: %s", error_message)
            logger.error("Fetching CloudWatch error log tail for run %s...", run_id)
            tail = fetch_job_error_logs(run_id)
            logger.error(
                "----- CloudWatch error log tail -----\n%s\n----- end log tail -----",
                tail,
            )
            raise RuntimeError(f"Job {job_name} ended in state {state}: {error_message}")

        time.sleep(POLL_INTERVAL_SECONDS)
        elapsed += POLL_INTERVAL_SECONDS

    raise TimeoutError(f"Job {job_name} did not reach a terminal state within {POLL_TIMEOUT_SECONDS}s")


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy and run a Glue job")
    parser.add_argument("--job-name", required=True, help="Glue job name, e.g. raw_to_iceberg")
    parser.add_argument("--script-path", required=True, help="Local path to the PySpark script")

    format_group = parser.add_mutually_exclusive_group()
    format_group.add_argument(
        "--iceberg", action="store_const", const="iceberg", dest="format_key",
        help="Iceberg confs only, Glue 4.0 (raw_to_iceberg.py)",
    )
    format_group.add_argument(
        "--iceberg-delta", action="store_const", const="iceberg_delta", dest="format_key",
        help="Both Iceberg (read) and Delta (write) confs, Glue 5.0 (iceberg_to_delta.py)",
    )
    format_group.add_argument(
        "--delta", action="store_const", const="delta", dest="format_key",
        help="Delta confs only, Glue 5.0 (compaction.py)",
    )

    parser.add_argument(
        "--glue-version", default=None,
        help="Override the Glue version implied by the format flag (rarely needed)",
    )
    parser.add_argument(
        "--test", action="store_true", help="Pass --test through to the job script (small sample, test table)"
    )
    parser.add_argument(
        "--test-limit", type=int, default=None,
        help="Pass --test-limit through to the job script (row cap when --test is set; job default is 10000)",
    )
    parser.add_argument(
        "--validate-only", action="store_true",
        help="Pass --validate-only through to the job script (read/cast full data, no write)",
    )
    args = parser.parse_args()

    glue_version = args.glue_version or (
        FORMAT_CONFIGS[args.format_key]["glue_version"] if args.format_key else DEFAULT_GLUE_VERSION
    )

    try:
        role_arn = get_glue_role_arn()
        script_s3_path = upload_script(args.script_path, args.job_name)
        create_or_update_job(args.job_name, script_s3_path, role_arn, args.format_key, glue_version)

        job_args = {}
        if args.test:
            job_args["--test"] = "true"
        if args.test_limit is not None:
            job_args["--test-limit"] = str(args.test_limit)
        if args.validate_only:
            job_args["--validate-only"] = "true"

        start_and_poll_job(args.job_name, extra_arguments=job_args or None)
    except (ClientError, RuntimeError, TimeoutError):
        logger.exception("Deploy/run failed for job %s", args.job_name)
        raise


if __name__ == "__main__":
    main()