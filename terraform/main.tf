terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "ap-south-1" # Mumbai — chosen over the eu-west-2 build-guide default
                         # to keep latency/cost low while developing from India.
                         # Region must stay consistent across S3, Glue, and any
                         # other AWS services used in this project.
}

# ---------------------------------------------------------------------------
# Variables
# ---------------------------------------------------------------------------

variable "bucket_name" {
  description = "Globally-unique S3 bucket name for the lakehouse."
  type        = string
  default     = "nyc-311-lakehouse"
}

# ---------------------------------------------------------------------------
# S3 bucket — single bucket, prefix-separated layers (raw / silver / gold /
# benchmarks). One bucket keeps IAM policy, versioning, and encryption config
# in one place rather than duplicated across three buckets. Iceberg (Stage 2)
# and Delta+UniForm (Stage 3) both live under silver/ and, thanks to UniForm,
# share the same underlying Parquet files rather than duplicating data.
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "lakehouse" {
  bucket = var.bucket_name
}

# Versioning enabled so accidental deletes/overwrites (e.g. a bad Glue job
# rerun during debugging) can be recovered rather than causing permanent data
# loss. Cheap insurance for a bucket that gets rewritten repeatedly during
# development.
resource "aws_s3_bucket_versioning" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Server-side encryption at rest. NYC 311 source data itself is public, but
# derived outputs (SLA breach analysis, agency performance scores) are this
# project's own work product — encrypting by default costs nothing and avoids
# having to reason about which prefixes "need" it later.
resource "aws_s3_bucket_server_side_encryption_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# All public access blocked. There's no reason for this bucket to be
# reachable outside this AWS account — all reads happen via Glue, DuckDB
# (with IAM credentials), or Snowflake external stages, never anonymous HTTP.
resource "aws_s3_bucket_public_access_block" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Lifecycle rule: move data to Glacier after 365 days. Benchmarks and raw
# ingestion snapshots older than a year have no ongoing analytical value for
# this project but are kept for reproducibility rather than deleted outright.
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse" {
  bucket = aws_s3_bucket.lakehouse.id

  rule {
    id     = "archive-after-1-year"
    status = "Enabled"

    filter {
      prefix = "" # applies bucket-wide
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }
  }
}

# Four folder prefixes as empty "directory marker" objects, matching the
# medallion-style layout: raw (Stage 1) -> silver (Stage 2 Iceberg + Stage 3
# Delta) -> gold (curated outputs) -> benchmarks (JSON results per stage).
resource "aws_s3_object" "prefixes" {
  for_each = toset(["raw/", "silver/", "gold/", "benchmarks/"])

  bucket  = aws_s3_bucket.lakehouse.id
  key     = each.value
  content = "" # zero-byte object purely to make the prefix visible in the console
}

# ---------------------------------------------------------------------------
# IAM role for AWS Glue
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "glue_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "nyc-311-lakehouse-glue-role"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
}

# Standard AWS-managed policy covering the baseline permissions any Glue job
# needs (CloudWatch logging, Glue API access, etc.).
resource "aws_iam_role_policy_attachment" "glue_service_role" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Scoped S3 read/write limited to this project's bucket only, rather than a
# blanket S3FullAccess grant on the role Glue jobs actually assume at
# runtime. The dev IAM user (used for aws configure / Terraform / boto3) has
# broader S3FullAccess by design for setup convenience, but the Glue
# execution role itself only needs this bucket.
data "aws_iam_policy_document" "glue_s3_access" {
  statement {
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.lakehouse.arn,
      "${aws_s3_bucket.lakehouse.arn}/*",
    ]
  }
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name   = "glue-s3-bucket-access"
  role   = aws_iam_role.glue_role.id
  policy = data.aws_iam_policy_document.glue_s3_access.json
}

# Glue Data Catalog full access needed because raw_to_iceberg.py and
# iceberg_to_delta.py both register/update table metadata in the Catalog
# (Iceberg and Delta tables under nyc_311.requests / nyc_311.requests_delta),
# not just read/write raw files.
resource "aws_iam_role_policy_attachment" "glue_catalog_access" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
}

# ---------------------------------------------------------------------------
# Glue Data Catalog database
# ---------------------------------------------------------------------------

resource "aws_glue_catalog_database" "nyc_311" {
  name = "nyc_311"
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "bucket_name" {
  value = aws_s3_bucket.lakehouse.id
}

output "glue_role_arn" {
  value = aws_iam_role.glue_role.arn
}
