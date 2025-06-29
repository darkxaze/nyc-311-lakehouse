"""Glue job: OPTIMIZE with Z-ORDER, then VACUUM the Delta table.

Z-ordering physically co-locates rows with similar borough/complaint_type
values within Parquet files. Additive to partition pruning — partition
pruning eliminates whole files, Z-ordering reduces bytes read *within*
files that do get opened. Different levels of the same problem.

VACUUM must run after OPTIMIZE: compaction writes new files but doesn't
immediately delete old ones (needed for Delta time-travel). Until VACUUM
runs, storage looks larger than it actually is — always measure storage
after VACUUM, never before.

Default retention is 168h (7 days): VACUUM will not delete anything on a
run immediately following OPTIMIZE — confirmed empirically during Stage 3
testing (0 files deleted, storage briefly doubled). A one-off dev run with
VACUUM_RETENTION_HOURS=0 and retentionDurationCheck disabled was used to
get accurate storage numbers immediately for benchmarking; production runs
use the safe 168h default below.

File count/size measured via direct S3 listing (boto3), not Spark's
_metadata.file_path/_metadata.file_size hidden columns — those require
Spark 4.0+, and Glue 5.0 runs Spark 3.5.4. Listing S3 directly under the
table path (excluding _delta_log/) is simpler and version-independent.

int96RebaseModeInWrite/datetimeRebaseModeInWrite set to CORRECTED:
OPTIMIZE rewrites Parquet files, so it can hit the same pre-1900
timestamp values that required this fix in iceberg_to_delta.py.

MaxRetries=0 set at job level.
"""

import logging
import sys

import boto3
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

AWS_REGION = "ap-south-1"
S3_BUCKET = "nyc-311-lakehouse"
DELTA_PATH = f"s3://{S3_BUCKET}/silver/311_delta/"
DELTA_TABLE_S3_PREFIX = "silver/311_delta/"
VACUUM_RETENTION_HOURS = 168  # 7 days — Delta default, preserves time-travel window


def build_spark_session() -> SparkSession:
    """Spark session with Delta extensions and ancient-date write handling.

    See module docstring: CORRECTED avoids INCONSISTENT_BEHAVIOR_CROSS_VERSION
    failures on pre-1900 timestamps present in the source data, which
    OPTIMIZE can re-encounter when rewriting files.
    """
    return (
        SparkSession.builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


def get_table_file_stats() -> tuple[int, int]:
    """Count and total size of active Parquet files under the table path.

    Direct S3 listing, not Spark's _metadata columns (Spark 4.0+ only —
    Glue 5.0 runs Spark 3.5.4). Excludes _delta_log/ — only data files
    count toward storage/file-count metrics.
    """
    s3 = boto3.client("s3", region_name=AWS_REGION)
    paginator = s3.get_paginator("list_objects_v2")

    file_count = 0
    total_bytes = 0
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=DELTA_TABLE_S3_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "_delta_log/" in key:
                continue
            file_count += 1
            total_bytes += obj["Size"]

    return file_count, total_bytes


def run_optimize(dt: DeltaTable) -> None:
    """OPTIMIZE with ZORDER on the two most common analytical filter columns."""
    logger.info("Running OPTIMIZE ZORDER BY (borough, complaint_type)")
    dt.optimize().executeZOrderBy("borough", "complaint_type")


def run_vacuum(dt: DeltaTable, retention_hours: int) -> None:
    logger.info("Running VACUUM (retention=%dh)", retention_hours)
    dt.vacuum(retentionHours=retention_hours)


def main() -> None:
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = build_spark_session()
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    dt = DeltaTable.forPath(spark, DELTA_PATH)

    files_before, bytes_before = get_table_file_stats()
    logger.info(
        "Before OPTIMIZE: %d files, %.2f GB (includes not-yet-vacuumed files from write)",
        files_before, bytes_before / (1024 ** 3),
    )

    run_optimize(dt)

    files_after_optimize, bytes_after_optimize = get_table_file_stats()
    logger.info(
        "After OPTIMIZE, before VACUUM: %d files, %.2f GB",
        files_after_optimize, bytes_after_optimize / (1024 ** 3),
    )

    run_vacuum(dt, VACUUM_RETENTION_HOURS)

    # Measure only after VACUUM — pre-VACUUM includes now-orphaned old files
    files_final, bytes_final = get_table_file_stats()
    logger.info(
        "After VACUUM: %d files, %.2f GB",
        files_final, bytes_final / (1024 ** 3),
    )

    job.commit()


if __name__ == "__main__":
    main()