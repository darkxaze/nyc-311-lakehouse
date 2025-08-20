# Future Work

Deliberately out of scope, not unfinished. This project's boundary is lakehouse engineering; everything below was excluded on purpose to keep that boundary sharp.

---

## 1. ML Layer — SLA Target Calibration

**Why this and not generic forecasting:** HPD Elevator breaches its 1-day target 80–93% of the time; HEAT/HOT WATER breaches its 7-day target under 1.3%. That gap says the target is miscalibrated, not that the agency is failing — a modelling question, not a query one.

**What it would do:** predict resolution-time distribution per complaint type at creation time, derive an empirical target (e.g. P80), and output published-vs-achievable per agency.

**Features already in the gold layer:** `complaint_type_normalized`, `borough`, `agency`, `created_date` (→ month/DOW/season), `resolution_days` (target), `is_anomaly` (exclusion filter), `target_days` (baseline), preserved coordinates (geo features).

**External data needed:** building attributes (MapPLUTO), HPD violation history, DOB elevator device records, NOAA weather — none confirmed against the current Open Data catalogue yet.

**Model:** gradient-boosted regression on `log(resolution_days)`, temporal split (train 2021–2024, validate 2025, not random — avoids seasonal leakage). Evaluate with per-type MAE and P80 calibration against a historical-median baseline.

**Why the lakehouse is a prerequisite:** feature iteration needs the 5.23s query, not the 291s one; the anomaly flag makes the training set excludable without silently poisoning the target; Delta time travel makes "trained on data as of X" verifiable; the coordinates LEFT-join was made specifically to keep this option open.

**Effort:** ~3–4 weeks — mostly acquiring and joining external building data.

---

## 2. Engineering Follow-ups

Each comes from a measured result that didn't go as expected:

- **dbt incremental scanned 22% *more* bytes than full refresh.** Cause unclear — Snowflake can't push pruning through the Iceberg-over-Delta catalog, or gold always fully rebuilds, or both. Isolate by re-running against Snowflake-managed Iceberg or Trino direct-on-Delta.
- **Query B is 50% slower on Delta than Iceberg** (22.53s → 33.85s) despite Z-ordering. Likely cause: Z-order on `(borough, complaint_type)` does nothing for a date-only filter, and the file count rose 383→691. Not yet measured — the most interesting open question in the project. Worth testing a Z-order variant that includes a date component.
- **UniForm remains deferred.** Glue Data Catalog can't satisfy the Thrift metastore requirement; revisit only if a persistent HMS is justified for other reasons.
- **Coordinates are ingested and joined but unused.** A Superset map layer would cost no new ingestion work.

---

## 3. Operational Hardening (production, not portfolio)

CI on dbt models · Soda failures should page, not just fail the DAG · Glue/Snowflake cost tracking · secrets manager instead of `.env` · adversarial backfill-overlap testing.

---

## 4. Explicitly Out of Scope

Streaming ingestion (daily data doesn't need it) · a serving API (output is a dashboard, not a product) · multi-region/DR (public API source can be re-ingested) · a third table format like Hudi (two is enough to make the point).
