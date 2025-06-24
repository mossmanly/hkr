{{
  config(
    materialized='view'
  )
}}

WITH expense_configs AS (
  SELECT * FROM {{ ref('stg_opex_expenses_inputs') }}
),

property_context AS (
  SELECT
    property_id,
    property_name,
    unit_count,
    company_id,
    portfolio_id
  FROM hkh_dev.stg_property_inputs
),

portfolio_totals AS (
  SELECT
    company_id,
    portfolio_id,
    SUM(unit_count) as total_portfolio_units
  FROM property_context
  GROUP BY company_id, portfolio_id
),

years_spine AS (
  SELECT generate_series(1, 20) as year
),

expense_calculations AS (
  SELECT
    pc.property_id,
    pc.property_name,
    pc.unit_count,
    pc.company_id,
    pc.portfolio_id,
    ec.category,
    ec.detail,
    ec.calculation_type,
    ys.year,
    ec.rate_value,
    ec.annual_escalation_rate,
    
    -- Escalated rate calculation
    ec.rate_value * POWER(1 + ec.annual_escalation_rate, ys.year - 1) as escalated_rate,
    
    -- Unit allocation percentage (for fixed expenses)
    ROUND(pc.unit_count::numeric / pt.total_portfolio_units, 4) as unit_allocation_pct,
    
    -- Final expense amount by calculation type
    CASE
      WHEN ec.calculation_type = 'per_unit' THEN
        pc.unit_count * ec.rate_value * POWER(1 + ec.annual_escalation_rate, ys.year - 1)
      WHEN ec.calculation_type = 'per_building' THEN
        ec.rate_value * POWER(1 + ec.annual_escalation_rate, ys.year - 1)
      WHEN ec.calculation_type = 'fixed' THEN
        (pc.unit_count::numeric / pt.total_portfolio_units) * ec.rate_value * POWER(1 + ec.annual_escalation_rate, ys.year - 1)
      ELSE 0
    END as expense_amount
    
  FROM expense_configs ec
  JOIN property_context pc ON ec.property_id = pc.property_id
  JOIN portfolio_totals pt ON pc.company_id = pt.company_id AND pc.portfolio_id = pt.portfolio_id
  CROSS JOIN years_spine ys
),

expense_calculations_with_totals AS (
  SELECT
    *,
    -- Window functions to calculate totals by property + year
    SUM(expense_amount) OVER (PARTITION BY property_id, year) as total_property_expenses,
    SUM(CASE WHEN calculation_type = 'per_unit' THEN expense_amount ELSE 0 END) OVER (PARTITION BY property_id, year) as per_unit_expenses,
    SUM(CASE WHEN calculation_type = 'per_building' THEN expense_amount ELSE 0 END) OVER (PARTITION BY property_id, year) as per_building_expenses,
    SUM(CASE WHEN calculation_type = 'fixed' THEN expense_amount ELSE 0 END) OVER (PARTITION BY property_id, year) as fixed_expenses,
    COUNT(*) OVER (PARTITION BY property_id, year) as expense_line_items
  FROM expense_calculations
)

SELECT
  property_id,
  property_name,
  unit_count,
  company_id,
  portfolio_id,
  category,
  detail,
  calculation_type,
  year,
  rate_value,
  annual_escalation_rate,
  ROUND(escalated_rate, 0) as escalated_rate,
  ROUND(unit_allocation_pct, 2) as unit_allocation_pct,
  ROUND(expense_amount, 0) as expense_amount,
  
  -- Summary fields by property + year (all dollars to 0 decimals)
  ROUND(total_property_expenses, 0) as total_property_expenses,
  ROUND(per_unit_expenses, 0) as per_unit_expenses,
  ROUND(per_building_expenses, 0) as per_building_expenses,
  ROUND(fixed_expenses, 0) as fixed_expenses,
  expense_line_items,
  
  CURRENT_TIMESTAMP as calculated_at,
  'fact_opex_expenses_calculations' as model_source
FROM expense_calculations_with_totals
ORDER BY property_id, category, detail, year