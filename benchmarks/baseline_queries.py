"""
Baseline query benchmarks over raw Parquet (Stage 1, pre-Iceberg/Delta).

Three queries representing different access patterns, each measured 3x
with the median recorded as the number that counts. These numbers are
the "before" side of every speedup claim made later in this project —
Iceberg (Stage 2) and Delta+Z-order (Stage 3) benchmarks reuse this exact
query shape so results are directly comparable.

Query A: full-history aggregation, no filters — worst case, scans
         every file. Tests the cost of having zero pruning available.
Query B: date-range + groupby — moderately selective, the kind of query
         partition pruning on created_date should help most with later.
Query C: borough + complaint_type + date range — most selective. This is
         the query used for the project's headline speedup number, since
         it benefits most from both partition pruning (Stage 2) and
         Z-ordering (Stage 3).

Before running the three benchmark queries, a row count is taken first.
This reads only Parquet footer metadata (each file stores its own row
count in the footer) — negligible S3 egress regardless of dataset size —
and confirms the full load actually landed correctly before spending
real time/egress on the more expensive benchmark queries.
"""

import argparse
import json
import logging
import os
import statistics
import time
from datetime import datetime, timezone
from typing import Callable

import duckdb
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

OUTPUT_PATH = "benchmarks/stage1_baseline.json"
RUNS_PER_QUERY = 3


def get_connection() -> duckdb.DuckDBPyConnection:
    """DuckDB connection configured for direct S3 Parquet reads via httpfs,
    explicitly pinned to ap-south-1. DuckDB's S3 support does NOT reliably
    infer region from AWS_DEFAULT_REGION alone for all operations, so it's
    set explicitly here — same category of bug as the dlt region-signing
    issue hit during ingestion.
    """
    bucket = os.getenv("S3_BUCKET")
    region = os.getenv("AWS_DEFAULT_REGION")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

    if not all([bucket, region, access_key, secret_key]):
        raise RuntimeError(
            "S3_BUCKET, AWS_DEFAULT_REGION, AWS_ACCESS_KEY_ID, and "
            "AWS_SECRET_ACCESS_KEY must all be set in .env"
        )

    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"SET s3_region='{region}';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    return con


def _glob_path(bucket: str) -> str:
    # Raw layer has no hive-style partitioning (see dlt_311_pipeline.py
    # docstring) — a flat wildcard glob is required, not hive_partitioning.
    return f"s3://{bucket}/raw/fetch_311_data/*.parquet"


def count_total_rows(con: duckdb.DuckDBPyConnection, bucket: str) -> int:
    """Row count via Parquet footer metadata only — negligible cost/egress,
    reads no actual column data. Sanity check that the full load landed
    correctly before running the (much more expensive) benchmark queries.
    """
    result = con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{_glob_path(bucket)}')
    """).fetchone()
    return result[0] if result else 0


def count_scanned_files(con: duckdb.DuckDBPyConnection, bucket: str) -> int:
    """Raw layer has no partition pruning at any layer, so every query
    scans every file — this count is the same for A, B, and C here. It
    becomes a meaningful comparison point once Iceberg/Delta can prune
    it down for Query C specifically.
    """
    result = con.execute(f"""
        SELECT COUNT(*) FROM glob('{_glob_path(bucket)}')
    """).fetchone()
    return result[0] if result else 0


def query_a(con: duckdb.DuckDBPyConnection, bucket: str) -> None:
    """Full history aggregation, no filters. Worst case: every file must
    be scanned in full, no pruning possible at any layer."""
    con.execute(f"""
        SELECT
            complaint_type,
            COUNT(*) AS volume,
            AVG(
                DATEDIFF(
                    'day',
                    TRY_CAST(created_date AS DATE),
                    TRY_CAST(closed_date AS DATE)
                )
            ) AS avg_resolution_days
        FROM read_parquet('{_glob_path(bucket)}')
        WHERE closed_date IS NOT NULL
        GROUP BY complaint_type
        ORDER BY volume DESC
        LIMIT 20
    """).fetchall()
    # TRY_CAST not CAST: 311 data has inconsistent date formats,
    # especially in older records. CAST throws and aborts the whole
    # query on the first unparseable date; TRY_CAST returns NULL for
    # that row and lets the aggregation continue over the rest.


def query_b(con: duckdb.DuckDBPyConnection, bucket: str) -> None:
    """Time series with date filter. Moderately selective — this is the
    shape of query that partition pruning on created_date should help
    with once we're on Iceberg (Stage 2)."""
    con.execute(f"""
        SELECT
            DATE_TRUNC('month', TRY_CAST(created_date AS DATE)) AS month,
            borough,
            COUNT(*) AS requests
        FROM read_parquet('{_glob_path(bucket)}')
        WHERE TRY_CAST(created_date AS DATE) >= '2022-01-01'
        GROUP BY 1, 2
        ORDER BY 1, 2
    """).fetchall()


def query_c(con: duckdb.DuckDBPyConnection, bucket: str) -> None:
    """Most selective filter: borough + complaint_type + date range.
    This is the query used for the project's headline speedup number —
    biggest expected improvement from partition pruning (Stage 2) and
    Z-ordering (Stage 3), since raw Parquet has no way to skip files or
    row groups on any of these predicates via folder structure (though
    DuckDB may still exploit Parquet's own per-row-group min/max stats
    for some pruning within each file)."""
    con.execute(f"""
        SELECT *
        FROM read_parquet('{_glob_path(bucket)}')
        WHERE borough = 'BROOKLYN'
        AND complaint_type = 'NOISE - RESIDENTIAL'
        AND TRY_CAST(created_date AS DATE) BETWEEN '2022-01-01' AND '2022-12-31'
    """).fetchall()


def time_query(
    query_fn: Callable[[duckdb.DuckDBPyConnection, str], None],
    con: duckdb.DuckDBPyConnection,
    bucket: str,
    runs: int = RUNS_PER_QUERY,
) -> float:
    """Run a query `runs` times, return the median wall-clock time in
    seconds. Median (not mean) to reduce sensitivity to one-off S3
    latency spikes or cold-cache effects on the first run.
    """
    timings = []
    for i in range(runs):
        start = time.perf_counter()
        query_fn(con, bucket)
        elapsed = time.perf_counter() - start
        timings.append(elapsed)
        logger.info("  run %d/%d: %.2fs", i + 1, runs, elapsed)
    return statistics.median(timings)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stage 1 baseline query benchmarks")
    parser.parse_args()

    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        raise RuntimeError("S3_BUCKET not set in .env")

    con = get_connection()

    logger.info("Counting scanned files...")
    files_scanned = count_scanned_files(con, bucket)
    logger.info("Files scanned per query: %d", files_scanned)

    logger.info("Counting total rows (metadata only, negligible cost)...")
    total_rows = count_total_rows(con, bucket)
    logger.info("Total rows in raw layer: %d", total_rows)

    results = {}

    for name, query_fn in [("query_a", query_a), ("query_b", query_b), ("query_c", query_c)]:
        logger.info("Running %s (%d runs)...", name, RUNS_PER_QUERY)
        median_time = time_query(query_fn, con, bucket)
        results[name] = {
            "median_seconds": round(median_time, 3),
            "files_scanned": files_scanned,
        }
        logger.info("%s median: %.2fs", name, median_time)

    results["timestamp"] = datetime.now(timezone.utc).isoformat()
    results["storage_format"] = "raw_parquet"
    results["total_rows"] = total_rows

    os.makedirs("benchmarks", exist_ok=True)
    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)
    logger.info("Results saved to %s", OUTPUT_PATH)

    print("\n" + "=" * 50)
    print("Stage 1 Baseline Benchmark Results (raw Parquet)")
    print("=" * 50)
    print(f"Total rows: {results['total_rows']:,}")
    print(f"{'Query':<10} {'Median (s)':<12} {'Files Scanned':<15}")
    for name in ["query_a", "query_b", "query_c"]:
        r = results[name]
        print(f"{name:<10} {r['median_seconds']:<12} {r['files_scanned']:<15}")
    print("=" * 50)


if __name__ == "__main__":
    main()