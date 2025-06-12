"""
NYC 311 Socrata API -> S3 Parquet via dlt.

Cursor pagination validated in scraps/test_pagination.py (Jan 2024:
287,186 rows / 6 pages, clean boundaries, no gaps/dupes).

Scope: 2021-01-01 through 2026-present. 2021-2025 is the clean trend
window; 2026 is partial and will be excluded from trend calcs downstream
(that exclusion happens in the dbt gold layer, not here — this script
ingests everything in scope).

created_date is safe as the cursor field because 311 records are
immutable after creation (Socrata doesn't retroactively edit created_date).

NOTE on partitioning: dlt's filesystem destination only supports a fixed
set of layout placeholders (table_name, load_id, file_id, ext, and
load-TIMESTAMP-based Y/M/D — not data-column-derived values). True
hive-style partitioning by created_date/borough is NOT available at this
layer. That's fine: this raw layer is for manageability only, not query
optimization. Iceberg (Stage 2) is where real partitioning happens, via
Glue/Spark reading the data columns directly — see months(created_date)
and truncate(borough,1) in glue_jobs/raw_to_iceberg.py.

NOTE on region: dlt's filesystem destination (s3fs/aiobotocore under the
hood) does NOT reliably inherit AWS_DEFAULT_REGION from the environment
the way the AWS CLI or plain boto3 does. Without an explicit region_name
passed via AwsCredentials, requests get signed for the wrong region and
S3 returns 400 Bad Request against non us-east-1 buckets (this project's
bucket is ap-south-1). Credentials are therefore built explicitly in
build_pipeline() rather than left to implicit discovery.

NOTE on dataset_name: must be a single valid identifier, not a path — dlt
silently sanitizes slashes (e.g. "raw/311" becomes "raw_311"). Bucket
prefix structure lives in bucket_url instead; dataset_name is just "raw".

NOTE on file format: the filesystem destination defaults to JSON Lines,
NOT Parquet, unless loader_file_format="parquet" is passed explicitly to
pipeline.run(). Every run() call below sets this explicitly since the
entire downstream project (benchmarks, Iceberg conversion) assumes the
raw layer is Parquet.

NOTE on write_disposition: the filesystem/Parquet destination does not
support true merge/upsert — only the "delta" table format does. Setting
write_disposition="merge" here silently falls back to "append" (dlt logs
a warning). This means:
  - Real deduplication on unique_key only happens at the Iceberg stage
    (Stage 2), which supports genuine MERGE INTO.
  - Re-running --test or --incremental against an already-loaded window
    WILL append duplicate rows to the raw layer. The year-level
    checkpoint is what prevents duplicates during a --full-load run;
    it does NOT protect ad-hoc --test/--incremental re-runs.
  - This is acceptable medallion-architecture behaviour (raw = append-only
    landing zone, silver = deduped) but must be an intentional, documented
    choice, not a surprise.
"""

import argparse
import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Iterator

import dlt
import requests
from dlt.common.configuration.specs import AwsCredentials
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

API_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
PAGE_SIZE = 50_000
YEARS_IN_SCOPE = [2021, 2022, 2023, 2024, 2025, 2026]
CHECKPOINT_PATH = Path(".checkpoints/ingestion_checkpoint.json")
MAX_RETRIES = 5
BASE_BACKOFF_SECONDS = 2
LOADER_FILE_FORMAT = "parquet"


def _load_checkpoint() -> dict:
    if CHECKPOINT_PATH.exists():
        return json.loads(CHECKPOINT_PATH.read_text())
    return {"completed_years": [], "in_progress_cursor": None}


def _save_checkpoint(state: dict) -> None:
    CHECKPOINT_PATH.parent.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_PATH.write_text(json.dumps(state, indent=2))


def _request_with_backoff(params: dict, headers: dict) -> list[dict]:
    """Retry with exponential backoff on transient request failures."""
    last_exc: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(API_ENDPOINT, params=params, headers=headers, timeout=60)
            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as exc:
            last_exc = exc
            wait = BASE_BACKOFF_SECONDS ** attempt
            logger.warning(
                "Request failed (attempt %d/%d): %s. Retrying in %ds.",
                attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)
    logger.error("Exhausted retries for params=%s", params)
    raise requests.exceptions.RequestException(
        f"Failed after {MAX_RETRIES} attempts against {API_ENDPOINT}"
    ) from last_exc


@dlt.resource(
    name="fetch_311_data",
    primary_key="unique_key",
    write_disposition="merge",
    columns={
        "unique_key": {"data_type": "text"},
        "created_date": {"data_type": "text"},
        "closed_date": {"data_type": "text"},
        "agency": {"data_type": "text"},
        "complaint_type": {"data_type": "text"},
        "borough": {"data_type": "text"},
        "status": {"data_type": "text"},
    },
)
def fetch_311_data(years: list[int]) -> Iterator[dict]:
    """Year-chunked, checkpointed pull. Called with a single-year list from
    main() so each dlt load package corresponds to exactly one year."""
    token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
    headers = {"X-App-Token": token} if token else {}

    state = _load_checkpoint()
    completed = set(state["completed_years"])

    for year in years:
        if year in completed:
            logger.info("Year %d already completed per checkpoint, skipping.", year)
            continue

        year_start = f"{year}-01-01T00:00:00"
        year_end = f"{year + 1}-01-01T00:00:00"

        cursor = state["in_progress_cursor"]
        resuming = cursor is not None and cursor.get("year") == year
        if resuming:
            last_date, last_key = cursor["created_date"], cursor["unique_key"]
            logger.info("Resuming year %d from cursor %s / %s", year, last_date, last_key)
        else:
            last_date, last_key = year_start, "0"

        rows_this_year = 0
        page = 0
        while True:
            # Matches the validated pattern in scraps/test_pagination.py:
            # the cursor condition is only added after the first page of
            # this year (or immediately on resume from checkpoint).
            where_clause = f"created_date >= '{year_start}' AND created_date < '{year_end}'"
            if page > 0 or resuming:
                where_clause += (
                    f" AND (created_date > '{last_date}' "
                    f"OR (created_date = '{last_date}' AND unique_key > '{last_key}'))"
                )

            params = {
                "$limit": PAGE_SIZE,
                "$order": "created_date,unique_key",
                "$where": where_clause,
            }

            records = _request_with_backoff(params, headers)
            if not records:
                logger.info("Year %d page %d: empty, stopping.", year, page)
                break

            for record in records:
                yield record

            rows_this_year += len(records)
            last_date, last_key = records[-1]["created_date"], records[-1]["unique_key"]

            state["in_progress_cursor"] = {
                "year": year, "created_date": last_date, "unique_key": last_key,
            }
            _save_checkpoint(state)

            logger.info(
                "Year %d page %d: %d rows, total %d, cursor=%s",
                year, page, len(records), rows_this_year, last_date,
            )

            if len(records) < PAGE_SIZE:
                break

            page += 1
            resuming = False  # only suppress the cursor clause on page 0 of a fresh year

        logger.info("Year %d complete: %d rows.", year, rows_this_year)
        state["completed_years"].append(year)
        state["in_progress_cursor"] = None
        _save_checkpoint(state)


@dlt.resource(
    name="fetch_311_data",
    primary_key="unique_key",
    write_disposition="merge",
    columns={
        "unique_key": {"data_type": "text"},
        "created_date": {"data_type": "text"},
        "closed_date": {"data_type": "text"},
        "agency": {"data_type": "text"},
        "complaint_type": {"data_type": "text"},
        "borough": {"data_type": "text"},
        "status": {"data_type": "text"},
    },
)
def fetch_311_data_test_window(start: str, end: str) -> Iterator[dict]:
    """Smoke-test resource: pulls a fixed date window, no checkpointing.
    Mirrors scraps/test_pagination.py exactly so output is directly
    comparable to the validated 287,186-row / 6-page result. Uses the
    same resource `name` as fetch_311_data so it lands in the same dlt
    table rather than creating a separate one.
    """
    token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
    headers = {"X-App-Token": token} if token else {}

    last_date, last_key = start, "0"
    page = 0
    total = 0

    while True:
        where_clause = f"created_date >= '{start}' AND created_date < '{end}'"
        if page > 0:
            where_clause += (
                f" AND (created_date > '{last_date}' "
                f"OR (created_date = '{last_date}' AND unique_key > '{last_key}'))"
            )
        params = {"$limit": PAGE_SIZE, "$order": "created_date,unique_key", "$where": where_clause}

        records = _request_with_backoff(params, headers)
        if not records:
            logger.info("Test window page %d: empty, stopping.", page)
            break

        for record in records:
            yield record

        total += len(records)
        last_date, last_key = records[-1]["created_date"], records[-1]["unique_key"]
        logger.info("Test window page %d: %d rows, total %d", page, len(records), total)

        if len(records) < PAGE_SIZE:
            break
        page += 1

    logger.info("Test window complete: %d rows (expect 287,186).", total)


def build_pipeline() -> dlt.Pipeline:
    bucket = os.getenv("S3_BUCKET")
    region = os.getenv("AWS_DEFAULT_REGION")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not bucket:
        raise RuntimeError("S3_BUCKET not set in environment (.env)")
    if not region:
        raise RuntimeError("AWS_DEFAULT_REGION not set in environment (.env)")
    if not access_key or not secret_key:
        raise RuntimeError(
            "AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY not set in environment (.env). "
            "Required explicitly here since credentials are no longer left to "
            "implicit discovery (see module docstring, region-signing bug)."
        )

    # Explicit credentials with region_name — see module docstring for why
    # this is required rather than relying on ~/.aws/credentials or
    # AWS_DEFAULT_REGION being picked up automatically by s3fs/aiobotocore.
    credentials = AwsCredentials(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    return dlt.pipeline(
        pipeline_name="nyc_311",
        destination=dlt.destinations.filesystem(
            # No "/raw" or "/raw/311" here — see module docstring on
            # dataset_name. bucket_url is just the bucket; dataset_name
            # below ("raw") is the single path segment dlt writes under,
            # giving a clean s3://<bucket>/raw/fetch_311_data/... layout.
            bucket_url=f"s3://{bucket}",
            layout="{table_name}/{load_id}.{file_id}.{ext}",
            credentials=credentials,
        ),
        dataset_name="raw",
        progress="log",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="NYC 311 ingestion pipeline")
    parser.add_argument("--full-load", action="store_true",
                         help="Run full year-chunked load across 2021-2026")
    parser.add_argument("--incremental", action="store_true",
                         help="Fetch only records since last run (uses dlt state)")
    parser.add_argument("--test", action="store_true",
                         help="Smoke test: pull Jan 2024 only, matches validated "
                              "scraps/test_pagination.py run (expect 287,186 rows)")
    args = parser.parse_args()

    pipeline = build_pipeline()
    start = time.perf_counter()

    try:
        if args.test:
            load_info = pipeline.run(
                fetch_311_data_test_window("2024-01-01T00:00:00", "2024-02-01T00:00:00"),
                loader_file_format=LOADER_FILE_FORMAT,
            )
            logger.info("Load info: %s", load_info)

        elif args.full_load:
            # One pipeline.run() per year: keeps dlt's own load packages
            # aligned with the year-level checkpointing in fetch_311_data,
            # so a crash mid-run only requires re-running the current year.
            for year in YEARS_IN_SCOPE:
                logger.info("Starting load for year %d", year)
                load_info = pipeline.run(
                    fetch_311_data([year]),
                    loader_file_format=LOADER_FILE_FORMAT,
                )
                logger.info("Year %d load info: %s", year, load_info)

        elif args.incremental:
            load_info = pipeline.run(
                fetch_311_data([datetime.now().year]),
                loader_file_format=LOADER_FILE_FORMAT,
            )
            logger.info("Load info: %s", load_info)

        else:
            parser.error("Specify --full-load, --incremental, or --test")
            return

    except requests.exceptions.RequestException as exc:
        logger.error("Ingestion failed against %s: %s", API_ENDPOINT, exc)
        raise

    elapsed = time.perf_counter() - start
    logger.info("Pipeline run complete in %.1fs", elapsed)


if __name__ == "__main__":
    main()