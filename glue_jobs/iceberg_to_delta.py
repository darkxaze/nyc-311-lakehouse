"""Glue job: converts Iceberg table to Delta.

UniForm (automatic Iceberg metadata generation) was attempted and
deliberately dropped from this job — see DECISIONS.md ADR on UniForm
deferral. Root cause: OSS Delta's UniForm Iceberg-conversion hook
(delta-iceberg jar) requires a literal Hive Metastore Thrift service.
AWS Glue Data Catalog's Hive-compatibility is client-factory-level, not
a real Thrift server, so Delta's shaded Iceberg HiveClientPool cannot
connect to it — confirmed via official docs.delta.io UniForm
requirements page and three independently root-caused job failures.

Column mapping (delta.columnMapping.mode=name) stays enabled: this
table is UniForm-ready if a real Hive Metastore is ever stood up later
(e.g. on EMR) — no schema migration would be needed at that point.

Required job parameters, set automatically by deploy_and_run.py --delta
(Glue 5.0 — bundles Delta 3.3.0/Spark 3.5.4):
  --datalake-formats delta
  --enable-glue-datacatalog true
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
  --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
  --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO
  --conf spark.sql.catalog.glue_catalog.warehouse=s3://nyc-311-lakehouse/silver/
MaxRetries=0 set at job level to avoid silent re-billing on failure.

--test writes to requests_delta_test (separate S3 path + catalog table)
instead of the real table, and caps input rows via --test-limit (default
10000). Mirrors the --test pattern in raw_to_iceberg.py so a bad config
change can be sanity-checked without touching production data.
"""

import argparse
import logging
import sys

from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_trunc, substring
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

S3_BUCKET = "nyc-311-lakehouse"
ICEBERG_TABLE = "glue_catalog.nyc_311.requests"
DEFAULT_TEST_LIMIT = 10000


def parse_optional_args() -> argparse.Namespace:
    """Parse --test/--test-limit/--validate-only manually.

    getResolvedOptions requires every listed arg to be present in
    sys.argv, so optional flags are parsed separately here rather than
    forcing deploy_and_run.py to always pass them.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--test", action="store_true")
    parser.add_argument("--test-limit", type=int, default=DEFAULT_TEST_LIMIT)
    parser.add_argument("--validate-only", action="store_true")
    known, _ = parser.parse_known_args(sys.argv[1:])
    return known


def build_spark_session() -> SparkSession:
    """Spark session with Delta extensions active.

    Iceberg extension kept alongside Delta's since reads still go
    through glue_catalog (Iceberg) even though writes are Delta-only now.

    int96RebaseModeInWrite/datetimeRebaseModeInWrite set to CORRECTED:
    the 311 dataset contains at least one row with a pre-1900 timestamp
    (likely a parsing artifact or genuine bad source data in a date
    field like due_date/resolution_action_updated_date). Spark's default
    Parquet writer refuses ancient dates without an explicit rebase
    mode, since Parquet's INT96 timestamp encoding predates the
    Gregorian-calendar convention Spark 3.0+ uses. CORRECTED writes the
    value as-is (no legacy-calendar compatibility needed here — nothing
    downstream reads these files with Spark 2.x or legacy Hive).
    """
    return (
        SparkSession.builder.config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension,"
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )


def add_partition_columns(df):
    """Derive Delta partition columns aligned with Iceberg's hidden partitioning.

    date_trunc('month', ...) not month(): month() alone loses year context
    (Jan 2022 and Jan 2023 would collide in the same partition). Same fix
    already applied in raw_to_iceberg.py for the same reason.

    substring(borough, 1, 1) mirrors Iceberg's truncate(borough, 1) so both
    formats prune identically on borough-filtered queries.
    """
    return df.withColumn(
        "created_month", date_trunc("month", "created_date")
    ).withColumn(
        "borough_truncated", substring("borough", 1, 1)
    )


def write_delta_table(df, catalog_table: str, path: str) -> None:
    """Write Delta with column mapping enabled (UniForm-ready, not UniForm-active).

    saveAsTable, not save(path): registers the table in the catalog as
    part of the write. Requires the target Glue database to have a
    non-empty LocationUri — Hive-client table registration fails
    otherwise (found and fixed during Stage 3 testing).

    repartition + sortWithinPartitions, not orderBy: orderBy does not
    guarantee partition-value locality across tasks on write (same issue
    hit during Iceberg writes in Stage 2).
    """
    (
        df.repartition("created_month", "borough_truncated")
        .sortWithinPartitions("created_month", "borough_truncated")
        .write.format("delta")
        .option("path", path)
        .option("delta.columnMapping.mode", "name")
        .partitionBy("created_month", "borough_truncated")
        .mode("overwrite")
        .saveAsTable(catalog_table)
    )


def main() -> None:
    resolved = getResolvedOptions(sys.argv, ["JOB_NAME"])
    opts = parse_optional_args()

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = build_spark_session()
    job = Job(glue_context)
    job.init(resolved["JOB_NAME"], resolved)

    # Test mode writes to a separate table/path — never touches production data.
    table_suffix = "_test" if opts.test else ""
    delta_path = f"s3://{S3_BUCKET}/silver/311_delta{table_suffix}/"
    # Unprefixed: resolves through spark_catalog (configured as DeltaCatalog),
    # not glue_catalog (configured as an Iceberg SparkCatalog).
    delta_catalog_table = f"nyc_311.requests_delta{table_suffix}"

    logger.info("Reading Iceberg source table: %s", ICEBERG_TABLE)
    df = spark.read.format("iceberg").load(ICEBERG_TABLE)

    if opts.test:
        logger.info("--test set: limiting to %d rows", opts.test_limit)
        df = df.limit(opts.test_limit)

    df_partitioned = add_partition_columns(df)

    if opts.validate_only:
        row_count = df_partitioned.count()
        logger.info("--validate-only set: read/cast succeeded, %d rows, no write performed", row_count)
        job.commit()
        return

    logger.info("Writing Delta table %s to %s", delta_catalog_table, delta_path)
    write_delta_table(df_partitioned, delta_catalog_table, delta_path)
    logger.info("Write complete.")

    job.commit()


if __name__ == "__main__":
    main()