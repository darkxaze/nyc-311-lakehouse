"""Measure Snowflake compute difference: full refresh vs incremental dbt run.

WARNING: takes ~3 hours. ACCOUNT_USAGE views lag live activity by up to
45min-3hrs (Snowflake's own documented latency, not a bug). Run once after
everything else is verified working -- not part of the normal dev loop.

Filters on query_text LIKE '%dbt%' because dbt prepends a JSON query
comment (/* {"app": "dbt", ...} */) to every query it issues, so this
reliably tags dbt-originated queries in QUERY_HISTORY.
"""

import json
import logging
import os
import subprocess
import time
from datetime import datetime

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

ACCOUNT_USAGE_LATENCY_WAIT_SECONDS = 3000  # ~50 min
BETWEEN_RUNS_WAIT_SECONDS = 0  # 2 hr, avoids overlapping query windows


def run_dbt_and_measure(full_refresh: bool) -> dict:
    cmd = ["dbt", "run", "--full-refresh"] if full_refresh else ["dbt", "run"]
    subprocess.run(cmd, check=True, cwd="dbt")

    logging.info(
        f"Waiting {ACCOUNT_USAGE_LATENCY_WAIT_SECONDS // 60} min for "
        "Snowflake ACCOUNT_USAGE latency..."
    )
    time.sleep(ACCOUNT_USAGE_LATENCY_WAIT_SECONDS)

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
    )
    try:
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT
                SUM(bytes_scanned) AS total_bytes,
                COUNT(*) AS query_count
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE query_text LIKE '%dbt%'
              AND start_time > DATEADD(hour, -1, CURRENT_TIMESTAMP())
            """
        )
        result = cursor.fetchone()
    finally:
        conn.close()

    return {
        "bytes_scanned": result[0] or 0,
        "query_count": result[1] or 0,
        "mode": "full_refresh" if full_refresh else "incremental",
    }


def main() -> None:
    full_refresh_metrics = run_dbt_and_measure(True)

    logging.info(
        f"Waiting {BETWEEN_RUNS_WAIT_SECONDS // 3600}hr between runs to "
        "avoid overlapping QUERY_HISTORY windows..."
    )
    time.sleep(BETWEEN_RUNS_WAIT_SECONDS)

    incremental_metrics = run_dbt_and_measure(False)

    full_bytes = full_refresh_metrics["bytes_scanned"]
    incr_bytes = incremental_metrics["bytes_scanned"]
    reduction_pct = ((full_bytes - incr_bytes) / full_bytes * 100) if full_bytes else 0.0

    results = {
        "full_refresh_bytes": full_bytes,
        "incremental_bytes": incr_bytes,
        "reduction_percentage": reduction_pct,
        "timestamp": datetime.now().isoformat(),
    }

    with open("benchmarks/stage4_dbt.json", "w") as f:
        json.dump(results, f, indent=2)

    print(f"Full refresh: {full_bytes:,} bytes")
    print(f"Incremental:  {incr_bytes:,} bytes")
    print(f"Reduction:    {reduction_pct:.1f}%")


if __name__ == "__main__":
    main()
    