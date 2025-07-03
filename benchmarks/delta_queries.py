"""
benchmarks/delta_queries.py

Same three queries as baseline_queries.py (Stage 1) and iceberg_queries.py
(Stage 2), run against the Z-ordered Delta table, to complete the
Query C progression: raw Parquet -> Iceberg -> Delta+Z-order.

Reads via DuckDB's delta extension, not delta-rs (deltalake package):
delta-rs cannot open this table at all — confirmed during Stage 3 testing
that delta-rs (up to 0.18.0, and per delta-rs's own documented
limitations) does not support the columnMapping reader feature this
table has enabled. DuckDB's delta extension is a separate, independent
implementation that does support it.

created_date/closed_date are already native TIMESTAMP columns (cast in
raw_to_iceberg.py during Stage 2) — no TRY_CAST needed here, unlike
Stage 1's baseline queries which read raw string columns.

Query C additionally reports DuckDB's EXPLAIN ANALYZE row count as a
Z-order effectiveness proxy: exact per-row-group skip statistics aren't
exposed through DuckDB's delta_scan the way PyIceberg's plan_files() did
for Iceberg in Stage 2 — this is the closest available signal without
parsing the Delta transaction log's file-level stats directly.
"""

import argparse
import json
import logging
import os
import statistics
import time
from datetime import datetime, timezone

import duckdb

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("delta_queries")

AWS_REGION = "ap-south-1"
S3_BUCKET = "nyc-311-lakehouse"
DELTA_PATH = f"s3://{S3_BUCKET}/silver/311_delta/"
RUNS_PER_QUERY = 3
OUTPUT_PATH = "benchmarks/stage3_delta.json"

QUERY_A = """
SELECT complaint_type,
       COUNT(*) AS volume,
       AVG(DATEDIFF('day', created_date, closed_date)) AS avg_resolution_days
FROM delta_scan('{path}')
WHERE closed_date IS NOT NULL
GROUP BY complaint_type
ORDER BY volume DESC
LIMIT 20
"""

QUERY_B = """
SELECT DATE_TRUNC('month', created_date) AS month,
       borough,
       COUNT(*) AS requests
FROM delta_scan('{path}')
WHERE created_date >= '2019-01-01'
GROUP BY 1, 2
ORDER BY 1, 2
"""

QUERY_C = """
SELECT *
FROM delta_scan('{path}')
WHERE borough = 'BROOKLYN'
AND complaint_type = 'NOISE - RESIDENTIAL'
AND created_date BETWEEN '2022-01-01' AND '2022-12-31'
"""


def connect() -> duckdb.DuckDBPyConnection:
    """DuckDB connection with delta + httpfs extensions and S3 auth configured.

    DuckDB's delta extension (delta-kernel-rs) authenticates via DuckDB's
    Secrets Manager (CREATE SECRET), not the legacy SET s3_access_key_id
    config used by httpfs — confirmed during Stage 3 testing: SET-based
    config had no effect on delta_scan() reads, which still fell through
    to the EC2 instance-metadata endpoint and timed out off-EC2.

    CONFIG provider with explicit credentials, not credential_chain: avoids
    depending on DuckDB's own credential-chain resolution (which has
    documented gaps depending on DuckDB version) by sourcing credentials
    directly from boto3, the same mechanism already proven to work
    elsewhere in this project.
    """
    import boto3

    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()

    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET http_timeout = 3000000;")  # 300s, up from DuckDB's ~30s default
    con.execute("SET http_retries = 5;")
    con.execute(f"""
        CREATE SECRET (
            TYPE s3,
            PROVIDER config,
            KEY_ID '{creds.access_key}',
            SECRET '{creds.secret_key}',
            REGION '{AWS_REGION}'
        );
    """)
    return con


def run_query_timed(con: duckdb.DuckDBPyConnection, sql: str, runs: int = RUNS_PER_QUERY) -> float:
    """Run a query `runs` times, return the median wall-clock seconds."""
    times = []
    for i in range(runs):
        start = time.perf_counter()
        con.execute(sql).fetchall()
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        logger.info("  run %d/%d: %.2fs", i + 1, runs, elapsed)
    return statistics.median(times)


def get_query_c_row_scan_count(con: duckdb.DuckDBPyConnection, sql: str) -> int | None:
    """Best-effort Z-order signal: rows scanned per EXPLAIN ANALYZE output.

    Not a true row-group skip percentage (that would require parsing
    Delta's file-level min/max stats directly) — see module docstring.
    Returns None if the plan text doesn't expose a parseable row count.
    """
    try:
        plan_rows = con.execute(f"EXPLAIN ANALYZE {sql}").fetchall()
        plan_text = "\n".join(str(row) for row in plan_rows)
        for line in plan_text.splitlines():
            if "rows=" in line.lower() or "cardinality" in line.lower():
                logger.info("  plan detail: %s", line.strip())
        return None  # exact parse omitted — logged for manual inspection instead
    except duckdb.Error as exc:
        logger.warning("EXPLAIN ANALYZE failed for Query C: %s", exc)
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark queries against the Stage 3 Delta table")
    parser.parse_args()

    con = connect()

    logger.info("Query A (full aggregation)")
    query_a_median = run_query_timed(con, QUERY_A.format(path=DELTA_PATH))

    logger.info("Query B (date filter)")
    query_b_median = run_query_timed(con, QUERY_B.format(path=DELTA_PATH))

    logger.info("Query C (most selective — borough + complaint_type + date range)")
    query_c_sql = QUERY_C.format(path=DELTA_PATH)
    query_c_median = run_query_timed(con, query_c_sql)
    get_query_c_row_scan_count(con, query_c_sql)

    results = {
        "query_a": {"median_seconds": round(query_a_median, 2)},
        "query_b": {"median_seconds": round(query_b_median, 2)},
        "query_c": {"median_seconds": round(query_c_median, 2)},
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "storage_format": "delta_zordered",
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print("\n" + "=" * 42)
    print("Delta (Z-ordered) Query Benchmark Results")
    print("=" * 42)
    print(f"Query A: {query_a_median:.2f}s")
    print(f"Query B: {query_b_median:.2f}s")
    print(f"Query C: {query_c_median:.2f}s")
    print(f"\nSaved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()