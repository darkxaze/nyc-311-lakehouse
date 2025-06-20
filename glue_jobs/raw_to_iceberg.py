"""
glue_jobs/raw_to_iceberg.py

Converts raw Parquet (Stage 1) to Iceberg with hidden partitioning.
AWS Glue 4.0 PySpark job.

REQUIRED at job submission (missing any = silent fallback to plain
Parquet, no Iceberg metadata written):
  --datalake-formats iceberg
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
  --conf spark.sql.catalog.glue_catalog.warehouse=s3://nyc-311-lakehouse/silver/

spark.sql.extensions is required for Iceberg to auto-cluster records by
partition value before a partitioned write. Without it, Spark writes
records in arrival order and Iceberg's ClusteredWriter throws
IllegalStateException ("Incoming records violate the writer
assumption..."). Discovered via a Stage 2 --test run failure. As a
defensive fallback (extension registration isn't always reliable after
session creation), write_to_iceberg() also explicitly sorts by the
partition columns before writing. See DECISIONS.md Stage 2.

Coordinates: originally planned to LEFT JOIN a dlt-normalized child
table for location.coordinates, but validated against the full
19M-row dataset that the main table's native latitude/longitude
columns already match it exactly (0 disagreements). Join dropped —
just cast the existing columns. See DECISIONS.md Stage 2.

Partitioning: written via CTAS with the partition spec in raw SQL
(not DataFrameWriterV2.partitionedBy) because pyspark.sql.functions
on Glue 4.0 (Spark 3.3) exports `months` but not `truncate`.
truncate(1, borough) collides BRONX/BROOKLYN (both 'B') — kept anyway
to demonstrate hidden partitioning; collision is measured, not hidden.

Modes:
  --validate-only : read + cast full dataset, print counts/schema, no write.
  --test           : small sample, writes to nyc_311.requests_test.
  (neither)        : full load, writes to nyc_311.requests.
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, substring, to_timestamp, trim, upper

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("raw_to_iceberg")

AWS_REGION = "ap-south-1"
S3_BUCKET = "nyc-311-lakehouse"

RAW_MAIN_PATH = f"s3://{S3_BUCKET}/raw/fetch_311_data/"

ICEBERG_TABLE = "glue_catalog.nyc_311.requests"
ICEBERG_TEST_TABLE = "glue_catalog.nyc_311.requests_test"
ICEBERG_WAREHOUSE = f"s3://{S3_BUCKET}/silver/"

TEMP_VIEW_NAME = "requests_staged"


def parse_args() -> argparse.Namespace:
    """parse_known_args ignores Glue's injected args (--JOB_NAME etc)."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true", help="Small sample, writes to requests_test")
    parser.add_argument("--test-limit", type=int, default=10000)
    parser.add_argument("--validate-only", action="store_true", help="Read/cast only, no write")
    args, _ = parser.parse_known_args()
    return args


def create_spark_session() -> SparkSession:
    """Iceberg/Glue catalog config, set defensively (Glue's job-parameter
    injection doesn't always propagate reliably). spark.sql.extensions is
    required for Iceberg's auto-clustering on partitioned writes — see
    module docstring."""
    return (
        SparkSession.builder.appName("raw_to_iceberg")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue_catalog.warehouse", ICEBERG_WAREHOUSE)
        .getOrCreate()
    )


def read_raw(spark: SparkSession):
    """Read main requests table. No coordinates join — see module docstring."""
    logger.info("Reading raw main table from %s", RAW_MAIN_PATH)
    df_main = spark.read.parquet(RAW_MAIN_PATH)
    logger.info("Read %d columns", len(df_main.columns))
    return df_main


def cast_columns(df):
    """to_timestamp (not cast) tolerates inconsistent date formats across
    the 2021-2026 window, returning NULL instead of throwing."""
    return (
        df.withColumn("created_date", to_timestamp(col("created_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .withColumn("closed_date", to_timestamp(col("closed_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))
        .withColumn("borough", upper(trim(col("borough"))))
        .withColumn("agency", trim(col("agency")))
        .withColumn("complaint_type", trim(col("complaint_type")))
        .withColumn("status", trim(col("status")))
        .withColumn("latitude", col("latitude").cast("double"))
        .withColumn("longitude", col("longitude").cast("double"))
    )


def write_to_iceberg(spark: SparkSession, df, table_name: str) -> None:
    """CTAS with SQL-native partition transforms. repartition + sort by
    date_trunc('month', created_date) — NOT pyspark's month() function,
    which only extracts calendar month (1-12) with no year component.
    Iceberg's months(created_date) transform partitions by year-month
    (April 2021 != April 2022), so clustering by month() alone let two
    different years' data interleave within a single Spark task,
    triggering ClusteredWriter's "already closed" error on the full
    19M-row load (didn't surface on the 10K --test run — not enough
    year-month spread in that small sample to collide). See
    DECISIONS.md Stage 2."""
    from pyspark.sql.functions import date_trunc

    df_clustered = df.repartition(
        date_trunc("month", col("created_date")), substring(col("borough"), 1, 1)
    ).sortWithinPartitions(
        date_trunc("month", col("created_date")), substring(col("borough"), 1, 1)
    )

    df_clustered.createOrReplaceTempView(TEMP_VIEW_NAME)
    logger.info("Writing to Iceberg table %s via CTAS", table_name)
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {table_name}
        USING iceberg
        PARTITIONED BY (months(created_date), truncate(1, borough))
        AS SELECT * FROM {TEMP_VIEW_NAME}
        """
    )
    logger.info("Write complete: %s", table_name)
    
def main() -> None:
    args = parse_args()
    spark = create_spark_session()
    try:
        df_typed = cast_columns(read_raw(spark))

        if args.validate_only:
            row_count = df_typed.count()
            logger.info("VALIDATE-ONLY: row count = %d", row_count)
            logger.info("VALIDATE-ONLY: schema:")
            df_typed.printSchema()

            null_coord_count = df_typed.filter(col("latitude").isNull()).count()
            null_pct = 100 * null_coord_count / row_count if row_count else 0.0
            logger.info(
                "VALIDATE-ONLY: %d of %d rows (%.1f%%) have null coordinates",
                null_coord_count, row_count, null_pct,
            )
            logger.info("VALIDATE-ONLY: skipping write. No Iceberg table touched.")
            return

        target_table = ICEBERG_TABLE
        if args.test:
            df_typed = df_typed.limit(args.test_limit)
            target_table = ICEBERG_TEST_TABLE
            logger.info("TEST MODE: limiting to %d rows, writing to %s", args.test_limit, target_table)

        logger.info("Total rows to write: %d", df_typed.count())
        write_to_iceberg(spark, df_typed, target_table)
    except Exception:
        logger.exception("raw_to_iceberg job failed")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()