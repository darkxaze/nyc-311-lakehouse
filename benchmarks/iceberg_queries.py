"""
benchmarks/iceberg_queries.py

Runs Query A/B/C (same as Stage 1 baseline_queries.py) against the
Iceberg table. For Query C, also logs files_opened_before_read from
the scan plan — proves pruning happens at metadata level before any
data file is touched.

Column projection (selected_fields) is used on every scan to limit
local memory usage — this workstation has 16GB RAM, and an unfiltered
scan with all ~48 columns pulled into PyArrow crashed the machine on
the first attempt. Only the columns each query actually needs are
selected. See DECISIONS.md Stage 2.

Query C uses borough='BROOKLYN', which only partially prunes due to
the BRONX/BROOKLYN truncate(1) collision. Expect solid but not
maximal improvement over the Stage 1 baseline (291.51s).
"""

import json
import logging
import os
import statistics
import time
from datetime import datetime, timezone

import duckdb
from pyiceberg.catalog import load_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("iceberg_queries")

AWS_REGION = "ap-south-1"
TABLE_NAME = "nyc_311.requests"
OUTPUT_PATH = "benchmarks/stage2_iceberg.json"
N_RUNS = 3


def load_iceberg_table():
    catalog = load_catalog(
        "glue",
        **{
            "type": "glue",
            "region_name": os.getenv("AWS_DEFAULT_REGION", AWS_REGION),
            "s3.access-key-id": os.getenv("AWS_ACCESS_KEY_ID"),
            "s3.secret-access-key": os.getenv("AWS_SECRET_ACCESS_KEY"),
        },
    )
    return catalog.load_table(TABLE_NAME)


def run_query_a(table) -> None:
    """Full aggregation, no filter — worst case, scans every file.
    Column-projected to (complaint_type, created_date, closed_date)
    only — the unfiltered full-column scan crashed this workstation."""
    arrow_table = table.scan(
        selected_fields=("complaint_type", "created_date", "closed_date")
    ).to_arrow()
    con = duckdb.connect()
    con.register("requests", arrow_table)
    con.execute(
        """
        SELECT complaint_type,
               COUNT(*) AS volume,
               AVG(DATE_DIFF('day', created_date, closed_date)) AS avg_resolution_days
        FROM requests
        WHERE closed_date IS NOT NULL
        GROUP BY complaint_type
        ORDER BY volume DESC
        LIMIT 20
        """
    ).fetchall()
    con.close()


def run_query_b(table) -> None:
    """Date-filtered time series — moderately selective.
    Column-projected to (created_date, borough) only.

    Timestamp literals require an explicit zone offset (+00:00) — the
    PyIceberg row_filter parser is stricter than DuckDB's SQL parser
    and rejects naive ISO-8601 strings with "Missing zone offset"."""
    scan = table.scan(
        row_filter="created_date >= '2019-01-01T00:00:00+00:00'",
        selected_fields=("created_date", "borough"),
    )
    arrow_table = scan.to_arrow()
    con = duckdb.connect()
    con.register("requests", arrow_table)
    con.execute(
        """
        SELECT DATE_TRUNC('month', created_date) AS month,
               borough,
               COUNT(*) AS requests
        FROM requests
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).fetchall()
    con.close()


def run_query_c(table) -> int:
    """Most selective filter: borough + complaint_type + date range.
    Returns files_opened from the scan plan. Timestamp literals need
    explicit +00:00 zone offset — see run_query_b docstring."""
    row_filter = (
        "borough == 'BROOKLYN' AND complaint_type == 'NOISE - RESIDENTIAL' "
        "AND created_date >= '2022-01-01T00:00:00+00:00' "
        "AND created_date <= '2022-12-31T23:59:59+00:00'"
    )
    scan = table.scan(row_filter=row_filter)
    files_opened = len(list(scan.plan_files()))
    arrow_table = scan.to_arrow()

    con = duckdb.connect()
    con.register("requests", arrow_table)
    con.execute("SELECT * FROM requests").fetchall()
    con.close()

    return files_opened


def time_query(fn, table, n_runs: int = N_RUNS) -> float:
    times = []
    for i in range(n_runs):
        start = time.perf_counter()
        fn(table)
        elapsed = time.perf_counter() - start
        times.append(elapsed)
        logger.info("  run %d/%d: %.2fs", i + 1, n_runs, elapsed)
    return statistics.median(times)


def main() -> None:
    table = load_iceberg_table()

    logger.info("Running Query A (full aggregation)...")
    median_a = time_query(run_query_a, table)

    logger.info("Running Query B (date filter)...")
    median_b = time_query(run_query_b, table)

    logger.info("Running Query C (most selective, borough+complaint+date)...")
    files_opened = None
    c_times = []
    for i in range(N_RUNS):
        start = time.perf_counter()
        files_opened = run_query_c(table)
        elapsed = time.perf_counter() - start
        c_times.append(elapsed)
        logger.info("  run %d/%d: %.2fs (files opened: %d)", i + 1, N_RUNS, elapsed, files_opened)
    median_c = statistics.median(c_times)

    results = {
        "query_a": {"median_seconds": round(median_a, 2)},
        "query_b": {"median_seconds": round(median_b, 2)},
        "query_c": {
            "median_seconds": round(median_c, 2),
            "files_opened_before_read": files_opened,
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "storage_format": "iceberg_partitioned",
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print("\n" + "=" * 50)
    print("Stage 2 — Iceberg Query Performance")
    print("=" * 50)
    print(f"Query A (full aggregation):   {median_a:.2f}s")
    print(f"Query B (date filter):        {median_b:.2f}s")
    print(f"Query C (most selective):     {median_c:.2f}s  (files opened: {files_opened})")
    print(f"\nSaved to {OUTPUT_PATH}")
    logger.info("Benchmark complete.")


if __name__ == "__main__":
    main()