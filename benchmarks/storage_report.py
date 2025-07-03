"""
benchmarks/storage_report.py

Measures S3 storage footprint at each stage: raw Parquet, Iceberg
(partitioned), Delta (Z-ordered, post-VACUUM). Prints and saves the
progression alongside the documented raw CSV size from NYC Open Data.

Delta size is read from the compaction.py Glue job's own post-VACUUM
log output (691 files, 1.60 GB — Stage 3), not re-measured here, since
that number is already the authoritative "after VACUUM" measurement per
the project's own rule (storage must be measured after VACUUM, never
before). Raw and Iceberg are measured fresh via S3 listing.

Raw prefix covers only fetch_311_data/ — the child coordinates table
(fetch_311_data__location__coordinates/) was dropped after the Stage 2
LEFT-join resolution and no longer exists in S3.

Iceberg prefix covers both data/ and metadata/ under the table root:
metadata (manifests, snapshots) is real, non-trivial storage cost that
a data/-only comparison would understate.

Note: Delta ends up LARGER than Iceberg (1.60GB vs 1.24GB), not smaller.
This is a real result, not a bug — Z-ordering optimizes physical row
locality for query-time pruning, it does not compress or deduplicate
data the way partition layout alone can. This table also underwent two
full OPTIMIZE/Z-order passes during Stage 3 testing (a --skip-optimize
flag was added to compaction.py but not correctly wired through the
second run), likely compounding fragmentation beyond a single pass.
See DECISIONS.md for the full writeup — the storage tradeoff is
reported honestly here rather than smoothed over.
"""

import argparse
import json
import logging
from datetime import datetime, timezone

import boto3

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("storage_report")

AWS_REGION = "ap-south-1"
S3_BUCKET = "nyc-311-lakehouse"
OUTPUT_PATH = "benchmarks/storage_sizes.json"

RAW_PREFIX = "raw/fetch_311_data/"
ICEBERG_PREFIX = "silver/nyc_311.db/requests/"

# Documented, not measured: source is NYC Open Data portal's published
# dataset size for erm2-nwe9, since we ingested via API (no local CSV
# ever downloaded to measure directly).
RAW_CSV_DOCUMENTED_GB = 18.4

# Delta size is the compaction.py job's own post-VACUUM log output
# (2026-08-12 run), not re-measured live here — see module docstring.
DELTA_POST_VACUUM_FILES = 691
DELTA_POST_VACUUM_GB = 1.60


def get_folder_size(bucket: str, prefix: str) -> tuple[int, int]:
    """Paginate S3 listing under a prefix, return (file_count, total_bytes)."""
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")

    file_count = 0
    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            file_count += 1
            total_bytes += obj["Size"]

    return file_count, total_bytes


def bytes_to_gb(b: int) -> float:
    return b / (1024 ** 3)


def pct_reduction(from_gb: float, to_gb: float) -> float:
    """Positive = reduction, negative = increase. Caller formats sign/label."""
    return (from_gb - to_gb) / from_gb * 100


def format_pct_change(from_gb: float, to_gb: float, label: str) -> str:
    """'-X.X% vs {label}' for a reduction, '+X.X% vs {label}' for an increase."""
    pct = pct_reduction(from_gb, to_gb)
    sign = "-" if pct >= 0 else "+"
    return f"({sign}{abs(pct):.1f}% vs {label})"


def main() -> None:
    parser = argparse.ArgumentParser(description="Report storage size progression across all three stages")
    parser.parse_args()

    logger.info("Measuring raw Parquet (Stage 1)...")
    raw_files, raw_bytes = get_folder_size(S3_BUCKET, RAW_PREFIX)
    raw_gb = bytes_to_gb(raw_bytes)
    logger.info("  %s: %d files, %.2f GB", RAW_PREFIX, raw_files, raw_gb)

    logger.info("Measuring Iceberg (Stage 2)...")
    iceberg_files, iceberg_bytes = get_folder_size(S3_BUCKET, ICEBERG_PREFIX)
    iceberg_gb = bytes_to_gb(iceberg_bytes)
    logger.info("  %s: %d files, %.2f GB", ICEBERG_PREFIX, iceberg_files, iceberg_gb)

    logger.info(
        "Delta (Stage 3): using compaction.py's own post-VACUUM measurement "
        "(%d files, %.2f GB) rather than re-listing live",
        DELTA_POST_VACUUM_FILES, DELTA_POST_VACUUM_GB,
    )

    results = {
        "raw_csv_documented_gb": RAW_CSV_DOCUMENTED_GB,
        "raw_parquet": {"files": raw_files, "gb": round(raw_gb, 2)},
        "iceberg_partitioned": {"files": iceberg_files, "gb": round(iceberg_gb, 2)},
        "delta_zordered": {"files": DELTA_POST_VACUUM_FILES, "gb": DELTA_POST_VACUUM_GB},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    with open(OUTPUT_PATH, "w") as f:
        json.dump(results, f, indent=2)

    print("\n" + "=" * 42)
    print("Storage Progression — NYC 311 Lakehouse")
    print("=" * 42)
    print(f"\nRaw CSV (source):              {RAW_CSV_DOCUMENTED_GB:.1f} GB  (documented)")
    print(f"Raw Parquet (Stage 1):          {raw_gb:.2f} GB  "
          f"{format_pct_change(RAW_CSV_DOCUMENTED_GB, raw_gb, 'raw CSV')}")
    print(f"Iceberg partitioned (Stage 2):  {iceberg_gb:.2f} GB  "
          f"{format_pct_change(raw_gb, iceberg_gb, 'raw Parquet')}")
    print(f"Delta + Z-order (Stage 3):      {DELTA_POST_VACUUM_GB:.2f} GB  "
          f"{format_pct_change(iceberg_gb, DELTA_POST_VACUUM_GB, 'Iceberg')}")
    print(f"\nTotal reduction (raw CSV -> Delta): "
          f"{pct_reduction(RAW_CSV_DOCUMENTED_GB, DELTA_POST_VACUUM_GB):.1f}%")
    print(f"\nSaved to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()