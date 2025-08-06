{{ config(materialized='ephemeral') }}

-- ephemeral: inlined as CTE into gold models, no storage, no incremental
-- state — exists only to keep gold SQL readable

SELECT
  r.unique_key,
  r.created_date,
  r.closed_date,
  r.borough,
  r.complaint_type_normalized,
  r.resolution_days,
  r.is_anomaly,
  r.anomaly_reason,

  s.responsible_agency,
  s.target_days,

  -- anomalies excluded from SLA calc (NULL, not FALSE): data quality
  -- issue, not a true service outcome
  CASE
    WHEN r.closed_date IS NULL THEN NULL
    WHEN r.is_anomaly THEN NULL
    WHEN r.resolution_days <= s.target_days THEN TRUE
    ELSE FALSE
  END AS is_within_sla,

  CASE
    WHEN r.closed_date IS NULL THEN NULL
    WHEN r.is_anomaly THEN NULL
    WHEN r.resolution_days > s.target_days THEN TRUE
    ELSE FALSE
  END AS is_sla_breach,

  GREATEST(r.resolution_days - s.target_days, 0) AS days_over_sla,

  EXTRACT(year FROM r.created_date) AS calendar_year

FROM {{ ref('stg_311_requests') }} r
INNER JOIN {{ ref('sla_targets') }} s
  ON r.complaint_type_normalized = s.complaint_type
  -- inner join, not left: complaint types with no documented SLA target
  -- have no breach to measure. A left join produced NULL breach_rate rows
  -- that ranked ahead of real values (Snowflake DESC sorts NULLs first),
  -- silently corrupting fct_sla_trends' top-10 ranking.