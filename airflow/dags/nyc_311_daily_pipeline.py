"""Daily NYC 311 pipeline: schema check -> incremental ingest -> conditional
compaction -> dbt run/test -> soda checks.

Tasks shell out to the project's existing scripts (ingestion/, dbt/, soda/)
rather than reimplementing logic in Airflow -- this DAG only orchestrates.
"""

import os

import boto3
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

PROJECT_DIR = "/opt/project"
S3_BUCKET = "nyc-311-lakehouse"
DELTA_PREFIX = "silver/311_delta/"
COMPACTION_FILE_THRESHOLD = 500


@dag(
    dag_id="nyc_311_daily_pipeline",
    schedule="0 6 * * *",
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False,
    tags=["nyc-311"],
)
def nyc_311_daily_pipeline():

    # runs first: catches breaking API schema changes before anything else
    # touches the data (exit 1 on breaking change fails the DAG here)
    check_schema = BashOperator(
        task_id="check_schema",
        bash_command=f"cd {PROJECT_DIR} && python ingestion/schema_tracker.py",
    )

    ingest_incremental = BashOperator(
        task_id="ingest_incremental",
        bash_command=f"cd {PROJECT_DIR} && python ingestion/dlt_311_pipeline.py --incremental",
    )

    @task.branch
    def check_file_count() -> str:
        s3 = boto3.client("s3")
        paginator = s3.get_paginator("list_objects_v2")
        file_count = sum(
            len(page.get("Contents", []))
            for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=DELTA_PREFIX)
        )
        return "run_compaction" if file_count > COMPACTION_FILE_THRESHOLD else "skip_compaction"

    @task(retries=5, retry_delay=pendulum.duration(seconds=10))
    def run_compaction() -> None:
        # Same ConcurrentRunsExceededException pattern documented in Stage 2
        # (actual_build.md): Glue's MaxConcurrentRuns=1 briefly rejects a new
        # start_job_run while it finishes releasing the previous run's slot
        # -- a benign timing issue, not real concurrency. deploy_and_run.py
        # already retries this with backoff; this task needs the same.
        boto3.client("glue").start_job_run(JobName="compaction")

    @task
    def refresh_snowflake_iceberg_metadata() -> None:
        # Compaction's VACUUM deletes old Parquet files. Snowflake's
        # Iceberg-over-Delta catalog integration can hold a stale cached
        # manifest pointing at a file VACUUM just deleted, causing dbt to
        # fail with "parquet file ... was inaccessible" -- confirmed live:
        # S3 listing showed the file genuinely gone, ALTER ICEBERG TABLE
        # ... REFRESH fixed it immediately. This forces that sync so dbt
        # never races against a stale manifest.
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
        )
        try:
            conn.cursor().execute(
                "ALTER ICEBERG TABLE staging.requests_delta REFRESH"
            )
        finally:
            conn.close()

    skip_compaction = BashOperator(
        task_id="skip_compaction",
        bash_command="echo 'file count under threshold, skipping compaction'",
    )

    run_dbt = BashOperator(
        task_id="run_dbt",
        bash_command=f"cd {PROJECT_DIR}/dbt && dbt run",
        trigger_rule="none_failed_min_one_success",  # proceed after either compaction branch
    )

    run_dbt_test = BashOperator(
        task_id="run_dbt_test",
        bash_command=f"cd {PROJECT_DIR}/dbt && dbt test",
    )

    run_soda_checks = BashOperator(
        task_id="run_soda_checks",
        bash_command=f"cd {PROJECT_DIR} && python soda/run_checks.py",
    )

    branch = check_file_count()
    compaction = run_compaction()
    refresh = refresh_snowflake_iceberg_metadata()
    check_schema >> ingest_incremental >> branch
    branch >> compaction >> refresh
    branch >> skip_compaction
    [refresh, skip_compaction] >> run_dbt >> run_dbt_test >> run_soda_checks


nyc_311_daily_pipeline()