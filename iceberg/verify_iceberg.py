"""
iceberg/verify_iceberg.py

Proves Iceberg partition pruning actually happens at the metadata level
(not just that data is decorated with partition columns). Compares files
scanned with a borough filter vs. an unfiltered scan.

truncate(1, borough) collapses BRONX/BROOKLYN into one partition value
(see raw_to_iceberg.py). A borough='BROOKLYN' filter still prunes out
MANHATTAN/QUEENS/STATEN ISLAND/UNSPECIFIED partitions, just not BRONX —
expect strong but not maximal pruning.
"""

import logging
import os

from pyiceberg.catalog import load_catalog

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("verify_iceberg")

AWS_REGION = "ap-south-1"
TABLE_NAME = "nyc_311.requests"
MIN_SKIP_PERCENTAGE = 50.0


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


def verify_partition_pruning(table) -> float:
    """Compare filtered vs. unfiltered file scan plans. Returns skip %."""
    scan_filtered = table.scan(row_filter="borough == 'BROOKLYN'")
    files_with_filter = len(list(scan_filtered.plan_files()))

    scan_all = table.scan()
    total_files = len(list(scan_all.plan_files()))

    skip_percentage = (total_files - files_with_filter) / total_files * 100

    assert skip_percentage > MIN_SKIP_PERCENTAGE, (
        f"Partition pruning not working: only skipping {skip_percentage:.1f}% "
        f"of files. Expected > {MIN_SKIP_PERCENTAGE}% for a single-borough filter, "
        f"even accounting for the BRONX/BROOKLYN truncate(1) collision."
    )

    logger.info(
        "Partition pruning verified: %.1f%% of files skipped (%d of %d files)",
        skip_percentage,
        total_files - files_with_filter,
        total_files,
    )
    return skip_percentage


def print_partition_summary(table) -> None:
    """List file counts per distinct partition value in the current snapshot."""
    scan = table.scan()
    partition_counts: dict = {}

    for task in scan.plan_files():
        key = str(task.file.partition)
        partition_counts[key] = partition_counts.get(key, 0) + 1

    logger.info("Partition summary (%d distinct partitions):", len(partition_counts))
    for key, file_count in sorted(partition_counts.items()):
        logger.info("  %s -> %d files", key, file_count)


def main() -> None:
    table = load_iceberg_table()
    verify_partition_pruning(table)
    print_partition_summary(table)


if __name__ == "__main__":
    main()