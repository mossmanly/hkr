-- File: models/debug/debug_portfolio_equity.sql

{{ config(materialized = 'table') }}

SELECT
  t.portfolio_id,
  /* Total Preferred Equity for this portfolio */
  SUM(
    CASE WHEN t.equity_class = 'Preferred' THEN t.equity_contributed ELSE 0 END
  ) AS preferred_equity,
  /* Total Common Equity for this portfolio */
  SUM(
    CASE WHEN t.equity_class = 'Common' THEN t.equity_contributed ELSE 0 END
  ) AS common_equity,
  /* Total Equity (preferred + common) */
  SUM(t.equity_contributed) AS total_equity,
  /* Weighted-average base_pref_irr */
  CASE
    WHEN SUM(t.equity_contributed) = 0 THEN 0
    ELSE ROUND(
           SUM(t.base_pref_irr * t.equity_contributed)
           / SUM(t.equity_contributed),
           4
         )
  END AS base_pref_irr
FROM {{ source('hkh_dev', 'tbl_terms') }} AS t
GROUP BY
  t.portfolio_id
ORDER BY
  t.portfolio_id