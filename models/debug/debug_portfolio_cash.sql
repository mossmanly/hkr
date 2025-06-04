-- File: models/debug/debug_portfolio_cash.sql

{{ config(materialized = 'table') }}

SELECT
  -- Normalize “micro-1” → “micro_1” so it matches debug_portfolio_equity
  REPLACE(pi.portfolio_id, '-', '_') AS portfolio_id,
  fpf.year,
  SUM(fpf.atcf) AS distributable_cash
FROM {{ ref('fact_property_cash_flow') }} AS fpf
LEFT JOIN {{ source('inputs', 'property_inputs') }} AS pi
  ON fpf.property_id = pi.property_id
GROUP BY
  REPLACE(pi.portfolio_id, '-', '_'),
  fpf.year
ORDER BY
  REPLACE(pi.portfolio_id, '-', '_'),
  fpf.year