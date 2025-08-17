"""Manual-trigger backfill for a historical date range. Separate from the
daily DAG to avoid race conditions with the scheduled pipeline -- run this
by hand via the Airflow UI (Trigger DAG w/ config), not on a schedule.
"""

from datetime import datetime

import boto3
import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task

PROJECT_DIR = "/opt/project"


@dag(
    dag_id="nyc_311_backfill",
    schedule=None,
    start_date=pendulum.datetime(2025, 8, 1, tz="UTC"),
    catchup=False,
    tags=["nyc-311", "manual"],
    params={"start_date": "", "end_date": ""},
)
def nyc_311_backfill():

    @task
    def validate_dates(**context) -> None:
        params = context["params"]
        start = datetime.fromisoformat(params["start_date"])
        end = datetime.fromisoformat(params["end_date"])
        if end < start:
            raise ValueError("end_date must be after start_date")

    run_backfill = BashOperator(
        task_id="run_backfill",
        bash_command=(
            f"cd {PROJECT_DIR} && python ingestion/dlt_311_pipeline.py "
            "--start-date {{ params.start_date }} --end-date {{ params.end_date }}"
        ),
    )

    @task(retries=5, retry_delay=pendulum.duration(seconds=10))
    def run_compaction() -> None:
        # Same ConcurrentRunsExceededException pattern as the daily
        # pipeline (see actual_build.md Stage 2 / Stage 5): Glue's
        # MaxConcurrentRuns=1 briefly rejects a new start_job_run while it
        # finishes releasing the previous run's slot.
        boto3.client("glue").start_job_run(JobName="compaction")

    @task
    def refresh_snowflake_iceberg_metadata() -> None:
        # Same fix as the daily pipeline: compaction's VACUUM can delete a
        # file Snowflake's Iceberg-over-Delta catalog integration still has
        # cached, causing dbt to fail with "parquet file ... was
        # inaccessible". Force a metadata sync before dbt runs.
        import os

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

    run_dbt_full_refresh = BashOperator(
        task_id="run_dbt_full_refresh",
        bash_command=f"cd {PROJECT_DIR}/dbt && dbt run --full-refresh",
    )

    validate_dates() >> run_backfill >> run_compaction() >> refresh_snowflake_iceberg_metadata() >> run_dbt_full_refresh


nyc_311_backfill()