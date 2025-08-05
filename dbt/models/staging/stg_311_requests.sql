{{
  config(
    materialized='incremental',
    unique_key='unique_key',
    incremental_strategy='merge',
    on_schema_change='fail'
  )
}}

-- on_schema_change=fail: loud break on Delta schema drift not caught by
-- schema_tracker.py first (second line of defence)

SELECT
  unique_key,
  created_date,
  closed_date,
  UPPER(TRIM(borough)) AS borough,
  TRIM(complaint_type) AS complaint_type,
  agency,
  status,

  -- normalize complaint types to match sla_targets seed keys
  CASE
    WHEN complaint_type ILIKE '%Noise%Residential%' THEN 'NOISE - RESIDENTIAL'
    WHEN complaint_type ILIKE '%Heat%' OR complaint_type ILIKE '%Hot Water%' THEN 'HEAT/HOT WATER'
    WHEN complaint_type ILIKE '%Unsanitary%' THEN 'UNSANITARY CONDITION'
    WHEN complaint_type ILIKE '%Illegal Parking%' THEN 'ILLEGAL PARKING'
    WHEN complaint_type ILIKE '%Water Leak%' THEN 'WATER LEAK'
    WHEN complaint_type ILIKE '%Rodent%' THEN 'RODENT'
    -- exact-string match on 'Street Condition' only: excludes compound
    -- variants (Root/Sewer/Sidewalk Condition, Homeless Street Condition,
    -- DEP Street Condition) which are meaningfully different complaint
    -- categories, not the same thing under a different name
    WHEN complaint_type = 'Street Condition' THEN 'STREET CONDITION'
    WHEN complaint_type = 'Sidewalk Condition' THEN 'SIDEWALK CONDITION'
    -- seed originally had 'BROKEN MUNI METER' -- doesn't exist in the real
    -- Socrata taxonomy. Actual complaint type is 'Broken Parking Meter'.
    -- sla_targets.csv corrected to match (see DECISIONS.md).
    WHEN complaint_type ILIKE '%Broken Parking Meter%' THEN 'BROKEN MUNI METER'
    ELSE UPPER(TRIM(complaint_type))
  END AS complaint_type_normalized,

  DATEDIFF('day', created_date, closed_date) AS resolution_days,

  -- anomaly = negative resolution (retro-merged), >730d (abandoned), or
  -- missing borough. Real data patterns, not errors — flag, don't drop.
  CASE
    WHEN closed_date < created_date THEN TRUE
    WHEN DATEDIFF('day', created_date, closed_date) > 730 THEN TRUE
    WHEN borough IS NULL OR TRIM(borough) = '' THEN TRUE
    ELSE FALSE
  END AS is_anomaly,

  CASE
    WHEN closed_date < created_date THEN 'retroactively merged case'
    WHEN DATEDIFF('day', created_date, closed_date) > 730 THEN 'abandoned case never properly closed'
    WHEN borough IS NULL OR TRIM(borough) = '' THEN 'missing borough value'
    ELSE NULL
  END AS anomaly_reason

FROM {{ source('nyc_311', 'requests_delta') }}

{% if is_incremental() %}
WHERE created_date > (SELECT MAX(created_date) FROM {{ this }})
{% endif %}