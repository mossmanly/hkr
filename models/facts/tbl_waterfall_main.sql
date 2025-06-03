{{ config(materialized = 'view') }}

WITH terms AS (
  SELECT
    LOWER(portfolio_id) AS portfolio_id,
    investor_serial AS investor_id,
    equity_contributed,
    base_pref_irr,
    equity_class
  FROM {{ source('hkh_dev','tbl_terms') }}
),

cash AS (
  SELECT
    LOWER(pi.portfolio_id) AS portfolio_id,
    fpf.year,
    SUM(fpf.atcf) AS alloc_cash
  FROM {{ ref('fact_property_cash_flow') }} fpf
  JOIN {{ source('inputs', 'property_inputs') }} pi
    ON fpf.property_id = pi.property_id
  GROUP BY LOWER(pi.portfolio_id), fpf.year
),

-- Create per-investor cash shares based on contributed equity within their class
investor_shares AS (
  SELECT
    c.portfolio_id,
    c.year,
    t.investor_id,
    t.equity_contributed,
    t.base_pref_irr,
    t.equity_class,
    c.alloc_cash,
    t.equity_contributed / SUM(t.equity_contributed) OVER (PARTITION BY c.portfolio_id, t.equity_class) AS class_ratio,
    ROW_NUMBER() OVER (PARTITION BY c.portfolio_id ORDER BY c.year) - 1 AS year_index
  FROM terms t
  JOIN cash c ON LOWER(t.portfolio_id) = c.portfolio_id
),

alloc_by_investor AS (
  SELECT *,
    ROUND(alloc_cash * class_ratio, 2) AS alloc_cash_investor
  FROM investor_shares
),

roc_calc AS (
  SELECT *,
    SUM(alloc_cash_investor) OVER (PARTITION BY portfolio_id, investor_id ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_roc,
    ROUND(
      CASE
        WHEN COALESCE(SUM(alloc_cash_investor) OVER (PARTITION BY portfolio_id, investor_id ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) < equity_contributed THEN
          LEAST(equity_contributed - COALESCE(SUM(alloc_cash_investor) OVER (PARTITION BY portfolio_id, investor_id ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0), alloc_cash_investor)
        ELSE 0
      END, 2
    ) AS roc_paid
  FROM alloc_by_investor
),

irr_setup AS (
  SELECT *,
    alloc_cash_investor - roc_paid AS available_for_irr,
    ROUND(POWER(1 + base_pref_irr, MAX(year_index) OVER (PARTITION BY portfolio_id, investor_id)) * equity_contributed - equity_contributed, 2) AS irr_total_due
  FROM roc_calc
),

irr_calc AS (
  SELECT *,
    SUM(available_for_irr) OVER (PARTITION BY portfolio_id, investor_id ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) AS prior_irr,
    ROUND(
      CASE
        WHEN COALESCE(SUM(available_for_irr) OVER (PARTITION BY portfolio_id, investor_id ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) >= irr_total_due THEN 0
        WHEN COALESCE(SUM(available_for_irr) OVER (PARTITION BY portfolio_id, investor_id ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) + available_for_irr <= irr_total_due THEN available_for_irr
        ELSE irr_total_due - COALESCE(SUM(available_for_irr) OVER (PARTITION BY portfolio_id, investor_id ORDER BY year ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0)
      END, 2
    ) AS irr_paid
  FROM irr_setup
),

promote_calc AS (
  SELECT *,
    ROUND(GREATEST(alloc_cash_investor - roc_paid - irr_paid, 0), 2) AS promote_pool
  FROM irr_calc
),

final_split AS (
  SELECT *,
    ROUND(promote_pool * 0.70, 2) AS investor_promote,
    ROUND(promote_pool * 0.30, 2) AS sponsor_promote
  FROM promote_calc
)

SELECT
  portfolio_id,
  year,
  investor_id,
  equity_class,
  equity_contributed,
  alloc_cash_investor,
  roc_paid,
  irr_paid,
  promote_pool,
  investor_promote,
  sponsor_promote,
  irr_total_due,
  prior_roc,
  prior_irr,
  CASE
    WHEN equity_contributed - prior_roc > 0 THEN 'ROC'
    WHEN prior_irr + irr_paid < irr_total_due THEN 'IRR'
    WHEN promote_pool > 0 THEN 'PROMOTE'
    ELSE 'UNALLOCATED'
  END AS cash_tier
FROM final_split
ORDER BY portfolio_id, investor_id, year