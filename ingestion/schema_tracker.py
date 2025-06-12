"""
NYC 311 Socrata API schema drift detector.

Runs first in every pipeline execution (see Kestra flow in Stage 5):
schema changes from Socrata are rare but not impossible, and detecting
them BEFORE ingestion runs prevents silently writing schema-incompatible
records into the raw layer. This is the first line of defence; dbt's
on_schema_change='fail' (Stage 4) is the second line, in case something
slips past this check.

Breaking change = column removed, or column type changed.
Non-breaking change = new optional column added.
"""

import argparse
import json
import logging
import os
import sys
from typing import Optional

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

API_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
SCHEMA_S3_KEY = "metadata/311_schema.json"


def fetch_current_schema() -> dict[str, str]:
    """Fetch one record from the API and derive a {column_name: python_type}
    schema from it. Socrata doesn't expose a clean schema endpoint for this
    dataset via SoQL, so we infer types from a live sample record instead —
    simpler and more honest than trusting stale API documentation.
    """
    token = os.getenv("NYC_OPEN_DATA_APP_TOKEN")
    headers = {"X-App-Token": token} if token else {}

    try:
        resp = requests.get(
            API_ENDPOINT,
            params={"$limit": 1},
            headers=headers,
            timeout=30,
        )
        resp.raise_for_status()
    except requests.exceptions.RequestException as exc:
        logger.error("Failed to fetch schema sample from %s: %s", API_ENDPOINT, exc)
        raise

    records = resp.json()
    if not records:
        raise RuntimeError(
            f"API returned zero records from {API_ENDPOINT}; cannot derive schema."
        )

    sample = records[0]
    return {key: type(value).__name__ for key, value in sample.items()}


def load_schema(bucket: str, s3_key: str) -> Optional[dict[str, str]]:
    """Read the previously saved schema from S3. Returns None if it
    doesn't exist yet (first-ever run)."""
    s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION"))
    try:
        response = s3.get_object(Bucket=bucket, Key=s3_key)
        return json.loads(response["Body"].read())
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "")
        if error_code in ("NoSuchKey", "404"):
            logger.info("No saved schema found at s3://%s/%s (first run).", bucket, s3_key)
            return None
        logger.error("Unexpected S3 error reading schema: %s", exc)
        raise


def save_schema(schema: dict[str, str], bucket: str, s3_key: str) -> None:
    """Write the current schema to S3 as the new baseline."""
    s3 = boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION"))
    try:
        s3.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(schema, indent=2).encode("utf-8"),
            ContentType="application/json",
        )
        logger.info("Saved schema to s3://%s/%s", bucket, s3_key)
    except ClientError as exc:
        logger.error("Failed to save schema to S3: %s", exc)
        raise


def compare_schemas(old: dict[str, str], new: dict[str, str]) -> tuple[bool, str]:
    """Compare two schemas. Returns (is_breaking, description).

    Breaking:
      - a column present in `old` is missing from `new` (column removal)
      - a column present in both has a different inferred type
    Non-breaking:
      - a column present in `new` but not `old` (new optional column)
    """
    old_keys = set(old.keys())
    new_keys = set(new.keys())

    removed = old_keys - new_keys
    added = new_keys - old_keys
    type_changes = {
        key: (old[key], new[key])
        for key in old_keys & new_keys
        if old[key] != new[key]
    }

    if removed or type_changes:
        parts = []
        if removed:
            parts.append(f"columns removed: {sorted(removed)}")
        if type_changes:
            change_desc = ", ".join(
                f"{col}: {old_t} -> {new_t}" for col, (old_t, new_t) in type_changes.items()
            )
            parts.append(f"type changes: {change_desc}")
        return True, "; ".join(parts)

    if added:
        return False, f"new optional columns added: {sorted(added)}"

    return False, "no schema changes detected"


def main() -> None:
    parser = argparse.ArgumentParser(description="NYC 311 API schema drift detector")
    parser.parse_args()

    bucket = os.getenv("S3_BUCKET")
    if not bucket:
        logger.error("S3_BUCKET not set in environment (.env)")
        sys.exit(1)

    try:
        current_schema = fetch_current_schema()
    except (requests.exceptions.RequestException, RuntimeError) as exc:
        logger.error("Could not determine current schema: %s", exc)
        sys.exit(1)

    saved_schema = load_schema(bucket, SCHEMA_S3_KEY)

    if saved_schema is None:
        save_schema(current_schema, bucket, SCHEMA_S3_KEY)
        logger.info("Initial schema saved. Nothing to compare against yet.")
        sys.exit(0)

    is_breaking, description = compare_schemas(saved_schema, current_schema)

    if is_breaking:
        logger.error("BREAKING SCHEMA CHANGE DETECTED: %s", description)
        sys.exit(1)
    elif description != "no schema changes detected":
        logger.warning("Non-breaking schema change: %s", description)
        save_schema(current_schema, bucket, SCHEMA_S3_KEY)
        sys.exit(0)
    else:
        logger.info("Schema unchanged.")
        sys.exit(0)


if __name__ == "__main__":
    main()