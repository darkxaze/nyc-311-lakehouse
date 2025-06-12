## Stage 1 Decisions

### Region: ap-south-1 (Mumbai) instead of guide default (eu-west-2)
**Context:** Build guide defaults to eu-west-2 for a UK-based workflow.
**Decision:** Use ap-south-1 throughout — S3, Glue, every AWS service touched
by this project.
**Rationale:** Lower latency/cost developing from India. Region must stay
fully consistent everywhere; a mismatch here was in fact the direct cause of
an S3 signature failure during ingestion, which reinforced why this has to
be enforced project-wide rather than assumed.

### Single bucket, prefix-separated layers
**Context:** Could use separate buckets per layer (raw/silver/gold/benchmarks).
**Decision:** One bucket, four prefixes.
**Rationale:** Centralizes versioning/encryption/IAM in one place. Delta
UniForm (Stage 3) shares underlying Parquet files with Iceberg under
silver/ anyway, so per-layer buckets would add operational complexity
without a real isolation benefit.

### No hive-style partitioning at the raw ingestion layer
**Context:** Original plan assumed year=YYYY/month=MM/ folders at raw
ingestion for manageability.
**Decision:** Dropped entirely — raw layer is a flat wildcard-glob Parquet
directory, no folder-based partitioning.
**Rationale:** dlt's filesystem destination only supports load-metadata
placeholders, not data-column-derived folders — but this also matches the
original intent better: real partitioning belongs at the Iceberg stage,
derived from actual column values (months(created_date),
truncate(borough,1)), not bolted onto raw ingestion as folder structure.

### Raw layer is append-only; deduplication deferred to Iceberg
**Context:** The filesystem/Parquet destination doesn't support true
merge/upsert — write_disposition="merge" silently falls back to append.
**Decision:** Accept this rather than build dedup logic at the raw layer.
**Rationale:** Standard medallion architecture — raw is an append-only
landing zone, real dedup on unique_key happens via genuine MERGE INTO at
the Iceberg stage (Stage 2). Practical consequence to remember: ad-hoc
--test/--incremental reruns against an already-loaded window will append
duplicates; only the year-level checkpoint protects --full-load reruns.

