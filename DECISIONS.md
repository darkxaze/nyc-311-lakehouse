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

### ADR-S3-1: UniForm deferred — AWS Glue lacks a real Hive Metastore
**Context:** Planned to enable Delta UniForm so the Delta table would also be readable as Iceberg, with zero data duplication.
**Discovery:** Three failures fixed in turn (missing `CatalogTable` on write, empty Glue database `LocationUri`, missing `--enable-glue-datacatalog`) — but the real blocker remained: UniForm's Iceberg-conversion hook requires a literal standalone Hive Metastore Thrift service, confirmed via `docs.delta.io`. AWS Glue Data Catalog's Hive-compatibility layer, used successfully everywhere else in the project, doesn't satisfy this.
**Decision:** Defer UniForm. Write Delta with `delta.columnMapping.mode=name` enabled (UniForm-ready, not active). No standalone HMS infrastructure added.
**Rationale:** A minimal HMS (small EC2 + RDS) would cost ~$25–45/month to unblock a feature the actual benchmarks don't depend on — Query C's 55.7x speedup works entirely without it.
**Consequence:** Iceberg and Delta remain independent tables sharing no metadata. Column mapping is enabled, so UniForm could be turned on later at zero migration cost.

### ADR-S3-2: Glue 5.0 for Delta jobs, Glue 4.0 stays for Iceberg
**Context:** UniForm needs Delta 3.0+, which needs Spark 3.5+. Glue 4.0 (Spark 3.3.0) bundles Delta 2.1.0 — no Delta version satisfies both requirements on Glue 4.0.
**Decision:** `iceberg_to_delta.py`/`compaction.py` run on Glue 5.0 (Spark 3.5.4, Delta 3.3.0). `raw_to_iceberg.py` stays on Glue 4.0, unchanged.
**Rationale:** No reason to touch an already-validated job; Iceberg's table format is version-independent, so a newer Iceberg reader opening an older writer's table is normal and supported.
**Consequence:** Two Glue engine versions run side by side by design, not oversight.

### ADR-S3-3: deploy_and_run.py's --uniform renamed to --iceberg-delta
**Context:** The flag configures dual-catalog access (Iceberg read + Delta write) — nothing to do with UniForm once UniForm was deferred.
**Decision:** Renamed `--uniform` → `--iceberg-delta`.
**Rationale:** A flag named after an abandoned feature would mislead future readers of the deploy script.
**Consequence:** No behavioral change — naming correction only.

### ADR-S3-4: verify_delta.py checks Delta-reader consistency, not Delta/Iceberg parity
**Context:** The planned `verify_uniform.py` would have proven identical row counts from a PyIceberg reader and a Delta reader on the same files — impossible once UniForm was deferred.
**Decision:** Rewrote as `verify_delta.py`, comparing row counts from two independent Delta readers (delta-rs and DuckDB's `delta` extension) instead.
**Rationale:** A narrower claim, but still a real check — two independent readers agreeing on the same `_delta_log` catches genuine log-replay bugs.
**Consequence:** Verification scope is narrower than originally planned; documented rather than silently downgraded.

### ADR-S3-5: Z-order compaction accepted despite increasing storage size
**Context:** Post-compaction Delta storage (1.60 GB) is 29.3% larger than Iceberg (1.24 GB), despite Query C improving 6.2x.
**Discovery:** Z-ordering optimizes row locality for query pruning, not storage size. Two full OPTIMIZE passes ran on this table during testing (a `--skip-optimize` flag wasn't correctly wired through one run), likely compounding the size increase.
**Decision:** Report the increase honestly rather than reframe or hide it.
**Rationale:** The tradeoff — storage cost for selective-query speed — is a legitimate, defensible finding, more credible than a report where every metric improved.
**Consequence:** Storage progression (2.42 GB → 1.24 GB → 1.60 GB) is non-monotonic but real, carried into the final report as-is.

### ADR-S3-6: One-off VACUUM retention override (0h), not left in committed code
**Context:** Delta's default 168h retention correctly blocked file deletion immediately after OPTIMIZE; an accurate storage number otherwise meant waiting 7 real days.
**Decision:** Ran `compaction.py` once with `VACUUM_RETENTION_HOURS=0` and the retention safety check disabled to force cleanup, then reverted both to the safe 168h default in the committed version.
**Rationale:** A week's wait wasn't practical for this timeline; the override was scoped to one ad hoc run, not the standing default.
**Consequence:** Reported storage numbers reflect an artificially early VACUUM — documented so the deviation is traceable.