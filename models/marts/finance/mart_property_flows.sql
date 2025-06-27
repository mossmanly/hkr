{{
  config(
    materialized='view'
  )
}}

SELECT 
  year,
  property_id,
  SUM(pgi) AS sum_of_pgi,
  SUM(egi) AS sum_of_egi, 
  SUM(noi) AS sum_of_noi,
  SUM(capex) AS sum_of_capex
FROM {{ ref('int_property_cash_flows') }}
GROUP BY year, property_id
ORDER BY year ASC, property_id ASC 