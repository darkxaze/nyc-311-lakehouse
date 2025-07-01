"""
delta/verify_delta.py

Verifies the Delta table by reading the same data with two independent
clients — delta-rs (Python, reads _delta_log directly) and DuckDB's
delta extension (a completely separate implementation) — and asserting
identical row counts.

UniForm was originally meant to make this an Iceberg-vs-Delta parity
check, proving true multi-format interoperability. UniForm was deferred
(see DECISIONS.md — AWS Glue Data Catalog doesn't satisfy OSS Delta's
Hive Metastore requirement for automatic Iceberg conversion). This
version checks Delta-client consistency instead: two independently
implemented Delta readers should agree on the same _delta_log. Smaller
claim than the original UniForm verification, but still a real
correctness check — a bug in one reader's log-replay logic would surface
as a mismatch here.
"""

import argparse
import logging
import os

import duckdb
from deltalake import DeltaTable

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("verify_delta")

AWS_REGION = "ap-south-1"
S3_BUCKET = "nyc-311-lakehouse"
DELTA_PATH = f"s3://{S3_BUCKET}/silver/311_delta/"


def get_delta_rs_row_count(version: int | None = None) -> tuple[int, int]:
    """Row count via delta-rs, pinned to a specific version if given.

    Returns (row_count, version_used).
    """
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY"),
        "AWS_REGION": os.getenv("AWS_DEFAULT_REGION", AWS_REGION),
    }
    dt = DeltaTable(DELTA_PATH, version=version, storage_options=storage_options)
    row_count = dt.to_pyarrow_table().num_rows
    return row_count, dt.version()


def get_duckdb_row_count() -> int:
    """Row count via DuckDB's delta extension — a separate implementation
    of Delta log replay, not delta-rs under the hood.
    """
    con = duckdb.connect()
    con.execute("INSTALL delta; LOAD delta;")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"""
        SET s3_region='{AWS_REGION}';
        SET s3_access_key_id='{os.getenv("AWS_ACCESS_KEY_ID")}';
        SET s3_secret_access_key='{os.getenv("AWS_SECRET_ACCESS_KEY")}';
    """)
    result = con.execute(f"SELECT COUNT(*) FROM delta_scan('{DELTA_PATH}')").fetchone()
    return result[0]


def verify() -> None:
    """Compare row counts from delta-rs and DuckDB, raise on mismatch."""
    delta_rs_count, version = get_delta_rs_row_count()
    logger.info("delta-rs row count (version %d): %s", version, f"{delta_rs_count:,}")

    duckdb_count = get_duckdb_row_count()
    logger.info("DuckDB delta_scan row count: %s", f"{duckdb_count:,}")

    if delta_rs_count != duckdb_count:
        raise AssertionError(
            f"Delta verification FAILED: delta-rs={delta_rs_count:,}, "
            f"duckdb={duckdb_count:,}, difference={abs(delta_rs_count - duckdb_count):,}"
        )

    logger.info("Delta VERIFIED: both clients return %s rows", f"{delta_rs_count:,}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Verify Delta table consistency between delta-rs and DuckDB readers"
    )
    parser.parse_args()
    verify()


if __name__ == "__main__":
    main()