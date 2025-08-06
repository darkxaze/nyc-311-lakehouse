{{ config(materialized='table', schema='gold') }}

SELECT
  complaint_type_normalized AS complaint_type,
  responsible_agency,
  calendar_year,
  target_days,

  COUNT(*) AS total_requests,
  SUM(CASE WHEN closed_date IS NOT NULL THEN 1 ELSE 0 END) AS closed_requests,
  SUM(CASE WHEN is_sla_breach THEN 1 ELSE 0 END) AS sla_breach_count,

  SUM(CASE WHEN is_sla_breach THEN 1 ELSE 0 END)::FLOAT /
    NULLIF(SUM(CASE WHEN closed_date IS NOT NULL THEN 1 ELSE 0 END), 0) AS sla_breach_rate,

  AVG(CASE WHEN NOT is_anomaly THEN resolution_days END) AS avg_resolution_days,
  PERCENTILE_CONT(0.5) WITHIN GROUP (
    ORDER BY CASE WHEN NOT is_anomaly THEN resolution_days END
  ) AS p50_resolution_days,
  PERCENTILE_CONT(0.9) WITHIN GROUP (
    ORDER BY CASE WHEN NOT is_anomaly THEN resolution_days END
  ) AS p90_resolution_days

FROM {{ ref('int_requests_enriched') }}
GROUP BY 1, 2, 3, 4
ORDER BY calendar_year, sla_breach_rate DESC
