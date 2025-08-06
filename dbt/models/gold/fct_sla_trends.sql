{{ config(materialized='table', schema='gold') }}

-- produces the project finding: top 10 complaint types by current breach
-- rate, with 5-year trend direction

WITH most_recent_year AS (
  -- 2026 excluded: partial year, mostly unresolved requests, breach rates
  -- unreliable. Locked scope: 2021-2025 is the clean trend window.
  SELECT MAX(calendar_year) AS max_year
  FROM {{ ref('fct_sla_performance') }}
  WHERE calendar_year <= 2025
),

top_10_current AS (
  SELECT complaint_type, responsible_agency
  FROM {{ ref('fct_sla_performance') }}
  WHERE calendar_year = (SELECT max_year FROM most_recent_year)
  ORDER BY sla_breach_rate DESC
  LIMIT 10
),

trends AS (
  SELECT
    p.complaint_type,
    p.responsible_agency,
    p.calendar_year,
    p.sla_breach_rate,
    p.target_days
  FROM {{ ref('fct_sla_performance') }} p
  INNER JOIN top_10_current t
    ON p.complaint_type = t.complaint_type
    AND p.responsible_agency = t.responsible_agency
  WHERE p.calendar_year >= (SELECT max_year FROM most_recent_year) - 4
)

SELECT
  complaint_type,
  responsible_agency,
  target_days,

  MAX(CASE WHEN calendar_year = (SELECT max_year FROM most_recent_year) - 4
      THEN sla_breach_rate END) AS breach_rate_y_minus_4,
  MAX(CASE WHEN calendar_year = (SELECT max_year FROM most_recent_year) - 3
      THEN sla_breach_rate END) AS breach_rate_y_minus_3,
  MAX(CASE WHEN calendar_year = (SELECT max_year FROM most_recent_year) - 2
      THEN sla_breach_rate END) AS breach_rate_y_minus_2,
  MAX(CASE WHEN calendar_year = (SELECT max_year FROM most_recent_year) - 1
      THEN sla_breach_rate END) AS breach_rate_y_minus_1,
  MAX(CASE WHEN calendar_year = (SELECT max_year FROM most_recent_year)
      THEN sla_breach_rate END) AS breach_rate_current,

  CASE
    WHEN MAX(CASE WHEN calendar_year = (SELECT max_year FROM most_recent_year)
         THEN sla_breach_rate END) <
         MAX(CASE WHEN calendar_year = (SELECT max_year FROM most_recent_year) - 4
         THEN sla_breach_rate END)
      THEN 'improving'
    WHEN MAX(CASE WHEN calendar_year = (SELECT max_year FROM most_recent_year)
         THEN sla_breach_rate END) >
         MAX(CASE WHEN calendar_year = (SELECT max_year FROM most_recent_year) - 4
         THEN sla_breach_rate END)
      THEN 'worsening'
    ELSE 'stable'
  END AS trend_direction

FROM trends
GROUP BY 1, 2, 3
ORDER BY breach_rate_current DESC