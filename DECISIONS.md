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

### ADR-S4-1: Snowflake trial on Asia Pacific (Singapore), accepting cross-region S3 reads
**Context:** Project's bucket is `ap-south-1` (Mumbai); Snowflake's free trial signup doesn't offer Mumbai as a region option, though it's generally supported.
**Decision:** Provisioned trial in Asia Pacific (Singapore) instead.
**Rationale:** Cross-region cost on a ~1.6GB table is negligible; Stages 1-3 already own this project's storage-performance benchmark story (same-region), so Snowflake's cross-region latency doesn't undercut it.
**Consequence:** Any Snowflake/dbt query time includes cross-region latency — not comparable to Stage 1-3's same-region Glue numbers.

### ADR-S4-2: Delta read via Iceberg-over-Delta catalog integration, not COPY INTO
**Context:** Needed Snowflake to query the Stage 3 Delta table without UniForm (deferred, ADR-S3-1).
**Options considered:** Legacy `EXTERNAL TABLE ... TABLE_FORMAT=DELTA` (Snowflake-deprecation-flagged); `CREATE ICEBERG TABLE` over an external volume + catalog integration; `COPY INTO` a native table.
**Decision:** External volume + catalog integration (`CATALOG_SOURCE=OBJECT_STORE`, `TABLE_FORMAT=DELTA`).
**Rationale:** COPY INTO would duplicate ~19M rows into Snowflake and undercut the lakehouse-vs-warehouse narrative; the legacy path is deprecation-flagged.
**Consequence:** S3 stays the single source of truth, read-only (`ALLOW_WRITES=FALSE`). Direct cause of ADR-S4-9's incremental-compute finding.

### ADR-S4-3: Dedicated IAM role for Snowflake, not reused from Glue
**Context:** Snowflake's external volume needs an assumable AWS role.
**Decision:** New role `snowflake-nyc311-delta-role`, read-only, scoped to `silver/311_delta/*` — not the existing Glue job role.
**Rationale:** Glue's trust policy is scoped to the Glue service principal; mixing purposes on one role is harder to reason about and rotate.
**Consequence:** Two roles to manage. Trust policy must be re-synced any time the external volume is recreated (Stage 4 bug #1).

### ADR-S4-4: Inner join to sla_targets, not left join
**Context:** `int_requests_enriched.sql` originally left-joined to the `sla_targets` seed, per the guide's template.
**Decision:** Changed to `INNER JOIN`.
**Rationale:** Complaint types with no seeded SLA target produced NULL breach rates that ranked first in Snowflake's default `DESC` sort (NULLs-first), corrupting `fct_sla_trends`' top-10 (bug #6). A type with no target has nothing to breach.
**Consequence:** Gold models only contain complaint types present in both the seed and the real data.

### ADR-S4-5: 2021–2025 window enforced in fct_sla_trends.sql, not just documented
**Context:** The locked 2021-2025 trend scope existed as stated intent but wasn't implemented as a filter in the gold model.
**Decision:** Capped `most_recent_year` at `<= 2025` directly in the model.
**Rationale:** Unbounded `MAX(calendar_year)` resolved to 2026 — mostly-NULL breach rates from a partial year broke the ranking (bug #5).
**Consequence:** 2026 stays available in `fct_sla_performance`; only `fct_sla_trends`' ranking is bounded.

### ADR-S4-6: Custom schema-naming macro; no default schema in Soda config
**Context:** dbt and Soda both produced doubled schema names (`staging_gold`, `staging.staging`) from the same root cause — concatenating a connection-level default schema with an already-qualified model/check schema.
**Decision:** Added `macros/generate_schema_name.sql` to use a model's configured schema as-is; removed the default `schema` key from Soda's connection YAML.
**Rationale:** Both tools' default "helpful" concatenation actively fights explicit, fully-qualified naming.
**Consequence:** Schemas now resolve exactly to `staging`/`gold` as intended.

### ADR-S4-7: Built against Soda Core 3.3.0's real API, not the guide's assumed methods
**Context:** The guide's `run_checks.py` calls `add_snowflake_connection()`, `get_checks_pass()`, `get_checks_pass_count()` — none exist in the installed 3.3.0 (three sequential `AttributeError`s).
**Decision:** Rewrote using `add_configuration_yaml_str()` and `has_check_fails()`/`get_checks_fail()` — the real API surface.
**Rationale:** No working alternative; fix forward against what's actually installed.
**Consequence:** Pass-count logging relies on Soda's own stdout scan summary, since no programmatic pass-count method exists in this version.

### ADR-S4-8: Real finding (Elevator, ~80-93% breach) reported instead of the guide's fabricated HEAT/HOT WATER number
**Context:** The guide's `CLAUDE.md` template pre-specifies a target finding (HEAT/HOT WATER "worsening" 23.4%→31.7%) that was never a measured result.
**Decision:** Report the real measured finding instead — HEAT/HOT WATER is actually improving (1.28%→0.43%); HPD Elevator's near-total, flat-to-worsening breach rate is the real headline.
**Rationale:** Matches this project's stated standard: every claim measured, not estimated.
**Consequence:** README/resume language should use the Elevator finding, not the guide's placeholder.

### ADR-S4-9: dbt incremental strategy doesn't reduce total pipeline compute here — reported as measured
**Context:** `dbt_performance.py` measured full-refresh vs. incremental bytes scanned, expecting a reduction per the guide's assumed target.
**Decision:** Reported the real result: incremental scanned **more** bytes (+22.1%), not less.
**Rationale:** The incremental filter still requires scanning the full external Iceberg-over-Delta source to evaluate (Snowflake can't push pruning through the catalog-integration layer, ADR-S4-2); both gold tables also fully rebuild every run regardless of staging's incrementality.
**Consequence:** A pruning-capable native Snowflake source would likely change this result — not tested, out of scope. Documented as an architecture-specific limitation, not a general claim against dbt incrementality.

### ADR-S4-10: Normalization fallback fixed to UPPER(TRIM()); seed's BROKEN MUNI METER corrected to match real taxonomy
**Context:** 3 of 10 seeded complaint types were missing from all gold output.
**Decision:** Changed the staging model's `ELSE complaint_type` fallback to `ELSE UPPER(TRIM(complaint_type))`; added exact-match rules for `Street Condition`/`Sidewalk Condition`; mapped the real `Broken Parking Meter` to the seed's `BROKEN MUNI METER` key.
**Rationale:** Real volume existed under mixed-case source strings that never matched the seed's uppercase keys; `BROKEN MUNI METER` doesn't exist in NYC's actual complaint-type taxonomy at all (guide error).
**Consequence:** All 10 types now surface. Side effect: the same case-sensitivity bug had also been silently excluding some Elevator rows — pre-fix numbers (95-97%) were on an incomplete slice; post-fix numbers (80-93%) are correct.


### ADR-S5-1: Airflow 3.x, not 2.x
**Context:** Airflow 2 reached end-of-life April 2026 — already 4 months past at
build time.
**Decision:** Built on Airflow 3.3.1.
**Rationale:** Building fresh on already-unsupported software would signal
stale practice, not current competence. Verified current 3.x docs (webserver
split into api-server, new Task Execution API, `airflow.sdk` imports) before
writing any DAG code, since most existing tutorials still skew toward 2.x
syntax.
**Consequence:** Real syntax drift from 2.x cost real debugging time (see
actual_build.md bugs #10-#11 — the Task Execution API's server URL and shared
JWT secret requirements are new in 3.x).

### ADR-S5-2: LocalExecutor, no Celery/Redis
**Context:** Airflow's official quick-start docker-compose defaults to
CeleryExecutor with Redis and multiple worker containers.
**Decision:** LocalExecutor, single Postgres for metadata, no Celery/Redis/
worker/triggerer services.
**Rationale:** Single-developer local setup doesn't need distributed task
execution — same reasoning already applied to Superset's local-only config in
the original guide.
**Consequence:** Simpler compose file, fewer moving parts, but not a pattern
that would scale to production multi-worker use without reintroducing Celery.

### ADR-S5-3: Airflow image built once manually, not via Compose's parallel build
**Context:** `x-airflow-common` originally had a `build:` key with a shared
`image:` tag; Compose's newer "bake" builder races multiple services trying to
build and tag the same image simultaneously.
**Decision:** Removed `build:` from the compose file. Image built once via a
plain `docker build -f Dockerfile.airflow -t nyc-311-airflow:latest .`
outside Compose; every service references the pre-built `image:` only.
**Rationale:** The parallel-build race left a corrupted image behind
(`ModuleNotFoundError: No module named 'airflow'` despite a clean build log) —
confirmed by isolating the same image via plain `docker run`, which worked
fine. Building once, outside Compose's parallel path, avoids the race
entirely.
**Consequence:** Requires an extra manual build step before `docker compose up`
rather than a single command; acceptable for local dev.

### ADR-S5-4: AIRFLOW_UID pinned to the image's own baked-in user (50000), not the host UID
**Context:** Airflow's standard advice sets `AIRFLOW_UID` to the host user's UID,
to avoid mounted-volume files being owned by an unexpected user.
**Decision:** Set `AIRFLOW_UID=50000` instead, matching the base image's own
`airflow` user.
**Rationale:** The custom Dockerfile's `pip install` runs during image build as
UID 50000 (the image's own default), and that user owns everything under
`/home/airflow/.local`. Running the container at a different UID at runtime
(the host's, e.g. 1000) couldn't cleanly see those installed packages —
confirmed by isolating `--user` and `--entrypoint` overrides independently,
neither alone reproduced the failure; only the real UID mismatch did.
**Consequence:** Mounted log/DAG files are owned by UID 50000 inside the
container rather than the host user — a real tradeoff of the standard advice,
acceptable for a single-developer local setup.

### ADR-S5-5: Two-stage pip install in Dockerfile.airflow — constrained vs. unconstrained
**Context:** Installing the full `requirements.txt` unconstrained conflicted
with Airflow's own pins (`polars`, `boto3`); using Airflow's constraints file
for everything then conflicted with `dbt-core`'s own `pathspec` pin.
**Decision:** Created `requirements-airflow.txt` (only what DAG tasks actually
invoke). Split its install: `dlt`/`boto3`/`python-dotenv`/`requests` *with*
Airflow's constraints file; `dbt`/`soda` *without*, in a separate step.
**Rationale:** No single strategy satisfied both Airflow's pins and this
project's tooling — splitting by actual dependency overlap resolved it.
**Consequence:** Two `RUN` layers instead of one; `setuptools` also needed a
third, separate unconstrained pin after the constrained stage broke `dlt`'s
import chain (actual_build.md bug #9).

### ADR-S5-6: Explicit shared EXECUTION_API_SERVER_URL and JWT_SECRET across all Airflow services
**Context:** Airflow 3's new Task Execution API requires every task process to
call back to the api-server over HTTP, signed with a JWT. Neither has a
sensible default in a multi-container Compose deployment.
**Decision:** Set `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=
http://airflow-api-server:8080/execution/` and one fixed
`AIRFLOW__API_AUTH__JWT_SECRET` value across every service in `x-airflow-common`.
**Rationale:** The default execution API URL (`localhost:8080`) resolves to the
wrong container from inside the scheduler; with no shared JWT secret, each
service independently generates its own random one, so tokens signed by one
service fail verification at another. Both are real, documented requirements
for Airflow 3's split-service architecture, not a workaround.
**Consequence:** The JWT secret is a static local-dev value; would need to be a
real, non-committed secret for any non-local deployment.

### ADR-S5-7: Skip `airflow users create`, rely on Simple Auth Manager
**Context:** `airflow users create` fails with `AttributeError:
'AirflowSecurityManagerV2' object has no attribute 'find_role'`.
**Decision:** Confirmed via web search this is a genuine, currently-open
upstream bug (`apache/airflow#51304`) — reproduced identically on a clean,
constraints-respecting rebuild. Dropped CLI user-creation entirely; Airflow 3's
Simple Auth Manager auto-generates an admin user on first `api-server`
startup, printed to its own logs.
**Rationale:** No official patch exists for this release; the workaround uses
Airflow's own built-in mechanism, not a manual DB edit.
**Consequence:** Auth credentials are auto-generated, read from container logs
on first startup — fine for local dev, not for a scripted deployment.

### ADR-S5-8: refresh_snowflake_iceberg_metadata task added between compaction and dbt
**Context:** Glue's `VACUUM` (part of `run_compaction`) deletes old Parquet
files; Snowflake's Iceberg-over-Delta catalog integration (ADR-S4-2) can hold a
stale cached manifest still pointing at a file that was just deleted, causing
`dbt run` to fail with "parquet file ... was inaccessible."
**Decision:** Added a dedicated task calling `ALTER ICEBERG TABLE ... REFRESH`
via `snowflake-connector-python`, placed between `run_compaction` and `run_dbt`
in both DAGs.
**Rationale:** Confirmed live — the referenced file was genuinely gone from S3
(direct `aws s3 ls` returned empty), and the manual `REFRESH` command
immediately fixed the same query that had failed identically on retry.
**Consequence:** Adds a small, cheap step to every run; without it, the
compaction → dbt sequence has a real, reproducible race condition inherent to
this architecture.

### ADR-S5-9: schema_tracker.py — multi-record sampling, symmetric Socrata metadata exclusion
**Context:** Single-record schema inference (Stage 1's original design)
produced two rounds of false-positive "breaking schema change" detections on
live data: real-but-sparse address fields absent from one sampled row, then
Socrata's own platform-computed `:@computed_region_*` columns.
**Decision:** Sample 100 records and union their keys instead of one. Exclude
any `:`-prefixed key (Socrata's convention for system/computed metadata, not
real dataset columns) from the comparison — applied symmetrically to both the
freshly-fetched schema and the previously-saved baseline, so an old baseline
captured before this filter existed can self-heal without a manual S3 reset.
**Rationale:** Confirmed both false positives directly against the live API
(`curl` sampling showed the fields present on other real records); the
computed-region columns are never read by any of this project's own transform
code regardless.
**Consequence:** Slightly higher per-run API cost (100 records instead of 1,
still negligible); the schema-drift check is now meaningfully more reliable
against Socrata's real, sparse-by-design data shape.

### ADR-S5-10: dlt_311_pipeline.py backfill support reuses the existing test-window resource
**Context:** The backfill DAG assumed `--start-date`/`--end-date` flags that had
never actually been implemented in `dlt_311_pipeline.py`.
**Decision:** Generalized the script's existing fixed-window `--test` resource
(`fetch_311_data_test_window`, renamed `fetch_311_data_range`) into a real
parameterized mode, rather than writing new fetch/pagination logic.
**Rationale:** The pagination pattern was already validated against a known
result (287,186 rows / Jan 2024); only the hardcoded window needed to become
configurable — reusing it avoids re-deriving correctness from scratch.
**Consequence:** No checkpointing on ad hoc backfills (matches `--test`'s
existing behavior) — a backfill is treated as a deliberate, supervised one-off
run, not something needing crash-resume.

### ADR-S5-11: Custom Dockerfile.superset for the Snowflake driver
**Context:** `snowflake-sqlalchemy` isn't included in the base
`apache/superset:3.0.0` image; installing it pulled in a `cryptography` version
conflicting with Superset's own pin, crashing the container at startup.
**Decision:** Custom `Dockerfile.superset` installs the driver, then explicitly
reinstalls `cryptography` back into Superset's supported range as a separate
step immediately after.
**Rationale:** Same two-stage pattern as `Dockerfile.airflow` — install the
thing that needs a newer shared dependency, then pin that dependency back down
for the packages that need the older range.
**Consequence:** One more image to build and maintain locally; the fix is
narrow and specific to this exact version combination, worth re-checking if
either Superset or snowflake-sqlalchemy is ever upgraded.