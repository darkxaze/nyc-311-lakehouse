### ADR-S1-1: Region ap-south-1 (Mumbai) instead of guide default (eu-west-2)
**Context:** Build guide defaults to eu-west-2 for a UK-based workflow.
**Decision:** Use ap-south-1 throughout — S3, Glue, every AWS service touched by this project.
**Rationale:** Lower latency/cost developing from India.
**Consequence:** Region must stay fully consistent everywhere; a mismatch here was in fact the direct cause of an S3 signature failure during ingestion (see Stage 1 bug #2), which reinforced why this has to be enforced project-wide rather than assumed.

### ADR-S1-2: Single bucket, prefix-separated layers
**Context:** Could use separate buckets per layer (raw/silver/gold/benchmarks).
**Decision:** One bucket, four prefixes.
**Rationale:** Centralizes versioning/encryption/IAM in one place.
**Consequence:** Delta UniForm (Stage 3) shares underlying Parquet files with Iceberg under `silver/` anyway, so per-layer buckets would add operational complexity without a real isolation benefit.

### ADR-S1-3: No hive-style partitioning at the raw ingestion layer
**Context:** Original plan assumed `year=YYYY/month=MM/` folders at raw ingestion for manageability.
**Decision:** Dropped entirely — raw layer is a flat wildcard-glob Parquet directory, no folder-based partitioning.
**Rationale:** dlt's filesystem destination only supports load-metadata placeholders, not data-column-derived folders (see Stage 1 bug #1).
**Consequence:** Matches the original intent better anyway — real partitioning belongs at the Iceberg stage, derived from actual column values (`months(created_date)`, `truncate(borough,1)`), not bolted onto raw ingestion as folder structure.

### ADR-S1-4: Raw layer is append-only; deduplication deferred to Iceberg
**Context:** The filesystem/Parquet destination doesn't support true merge/upsert — `write_disposition="merge"` silently falls back to append (see Stage 1 bug #6).
**Decision:** Accept this rather than build dedup logic at the raw layer.
**Rationale:** Standard medallion architecture — raw is an append-only landing zone, real dedup on `unique_key` happens via genuine `MERGE INTO` at the Iceberg stage (Stage 2).
**Consequence:** Ad-hoc `--test`/`--incremental` reruns against an already-loaded window will append duplicates; only the year-level checkpoint protects `--full-load` reruns.

### ADR-S2-1: Coordinates join dropped in favor of flat lat/long columns
**Context:** Original plan (locked pre-Stage 2) was to LEFT JOIN the dlt-normalized `location.coordinates` child table back onto the main requests table.
**Discovery:** The main table already has flat `latitude`/`longitude` string columns from the Socrata API. Validated via `--validate-only` comparison: 0 disagreements across 18,761,797 rows with both sources present.
**Decision:** Drop the join. Cast the existing flat columns directly.
**Consequence:** One fewer S3 read (~1.67GB coordinates table) and one fewer join on every run, no loss of data fidelity.

### ADR-S2-2: truncate(1, borough) kept despite BRONX/BROOKLYN collision
**Context:** `truncate(borough, 1)` collapses BRONX and BROOKLYN into the same partition value (both start with 'B').
**Options considered:** `truncate(3)` or `identity(borough)` for full separation.
**Decision:** Kept `truncate(1)` — demonstrates hidden partitioning per the build guide; the collision is measured (verified 80.3% skip rate still achieved), not hidden.
**Consequence:** Brooklyn-filtered queries prune out Manhattan/Queens/Staten Island/Unspecified but not Bronx. Query C's 12-file scan and 9.04x speedup confirm this is still highly effective in practice.

### ADR-S2-3: CTAS with raw SQL partition spec, not DataFrameWriterV2.partitionedBy()
**Context:** `pyspark.sql.functions.truncate` is not exported on Spark 3.3 (Glue 4.0), though `months` is.
**Decision:** Write via `CREATE OR REPLACE TABLE ... PARTITIONED BY (months(...), truncate(1, ...))` SQL, not Python transform helpers.
**Consequence:** Avoids the PySpark API version gap entirely; more portable across Spark/Glue versions.

### ADR-S2-4: repartition + sort by date_trunc('month', ...), not month()
**Context:** Iceberg's `ClusteredWriter` requires all rows for a given partition value to be handled by one Spark task. `month()` extracts only calendar month (1–12), causing April-2021 and April-2022 rows to cluster together incorrectly — passed a 10K-row test, failed on the full 19M-row load.
**Decision:** Use `date_trunc('month', created_date)` (year+month) as the repartition/sort key, matching what Iceberg's `months()` transform actually partitions on.
**Consequence:** Full load succeeded. Lesson: small-sample tests can pass despite a partition-clustering bug if the sample doesn't span enough distinct partition values — validated the fix at 200K rows before the full run.

### ADR-S2-5: MaxRetries=0 and boto3 deploy script over raw CLI
**Context:** Glue's default retry re-runs the entire job from scratch on failure, silently re-billing DPU-hours on non-transient bugs.
**Decision:** `deploy_and_run.py` (boto3, reusable across stages) sets `MaxRetries=0` explicitly, reads the Glue role ARN from `terraform output` (not hardcoded), and auto-fetches CloudWatch logs on failure.
**Consequence:** Failures are visible and cheap to diagnose; no silent cost from repeated retries during active debugging.