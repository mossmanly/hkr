{{
  config(
    materialized='table'
  )
}}

WITH portfolio_defaults AS (
  SELECT
    company_id,
    portfolio_id,
    category,
    detail,
    calculation_type,
    rate_value,
    annual_escalation_rate
  FROM hkh_dev.stg_unit_based_expenses
),

property_universe AS (
  SELECT
    property_id,
    property_name,
    unit_count,
    company_id,
    portfolio_id
  FROM hkh_dev.stg_property_inputs
),

property_expense_expansion AS (
  SELECT
    pd.company_id,
    pd.portfolio_id,
    pu.property_id,
    pu.property_name,
    pu.unit_count,
    pd.category,
    pd.detail,
    pd.calculation_type,
    pd.rate_value,
    pd.annual_escalation_rate,
    CURRENT_TIMESTAMP as created_at,
    'stg_opex_expenses_inputs' as model_source
  FROM portfolio_defaults pd
  CROSS JOIN property_universe pu
  WHERE pd.company_id = pu.company_id
    AND pd.portfolio_id = pu.portfolio_id
)

SELECT * FROM property_expense_expansion
ORDER BY property_id, category, detail 