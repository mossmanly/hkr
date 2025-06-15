{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['assumption_category'], 'unique': false},
      {'columns': ['portfolio_id'], 'unique': false}
    ]
  )
}}

-- Extract valuation assumptions - streamlined version
-- References only staging models for clean data lineage

WITH market_parameters AS (
  SELECT 
    parameter_name,
    parameter_value,
    parameter_description,
    effective_date
  FROM hkh_dev.stg_market_parameters
),

portfolio_settings AS (
  SELECT 
    company_id,
    portfolio_id,
    portfolio_name,
    investment_strategy,
    is_default
  FROM hkh_dev.stg_portfolio_settings
  WHERE is_active = TRUE
),

-- Market assumptions
market_assumptions AS (
  SELECT 
    'market_rates' AS assumption_category,
    parameter_name AS assumption_name,
    parameter_value AS assumption_value,
    parameter_description AS assumption_description,
    effective_date AS assumption_date,
    
    CASE 
      WHEN parameter_name LIKE '%cap_rate%' THEN 'capitalization_rates'
      WHEN parameter_name LIKE '%rent%' THEN 'rental_rates' 
      WHEN parameter_name LIKE '%growth%' THEN 'growth_assumptions'
      WHEN parameter_name LIKE '%cost%' THEN 'cost_assumptions'
      ELSE 'other_market_assumptions'
    END AS assumption_subcategory,
    
    CASE 
      WHEN parameter_name IN ('cap_rate_range_low', 'cap_rate_range_high', 'exit_cap_rate') THEN 'high_impact'
      WHEN parameter_name IN ('annual_rent_growth', 'annual_expense_growth') THEN 'medium_impact'
      ELSE 'low_impact'
    END AS assumption_risk_level
    
  FROM market_parameters
),

-- Portfolio strategy assumptions
portfolio_assumptions AS (
  SELECT 
    ps.company_id,
    ps.portfolio_id,
    'portfolio_strategy' AS assumption_category,
    'investment_strategy' AS assumption_name,
    0 AS assumption_value, -- Placeholder for text strategies
    CONCAT('Investment strategy: ', ps.investment_strategy) AS assumption_description,
    CURRENT_DATE AS assumption_date,
    'strategy_assumptions' AS assumption_subcategory,
    'high_impact' AS assumption_risk_level
    
  FROM portfolio_settings ps
),

-- Combine assumptions
all_assumptions AS (
  SELECT 
    NULL AS company_id,
    NULL AS portfolio_id,
    assumption_category,
    assumption_name,
    assumption_value,
    assumption_description,
    assumption_date,
    assumption_subcategory,
    assumption_risk_level
  FROM market_assumptions
  
  UNION ALL
  
  SELECT 
    company_id,
    portfolio_id,
    assumption_category,
    assumption_name,
    assumption_value,
    assumption_description,
    assumption_date,
    assumption_subcategory,
    assumption_risk_level
  FROM portfolio_assumptions
),

-- Add metadata
final_assumptions AS (
  SELECT 
    *,
    
    -- Assumption source
    CASE 
      WHEN assumption_category = 'market_rates' THEN 'market_data'
      WHEN assumption_category = 'portfolio_strategy' THEN 'portfolio_configuration'
      ELSE 'unknown'
    END AS assumption_source,
    
    -- Update frequency
    CASE 
      WHEN assumption_subcategory LIKE '%cap_rate%' THEN 'quarterly'
      WHEN assumption_subcategory LIKE '%growth%' THEN 'annual'
      WHEN assumption_subcategory LIKE '%strategy%' THEN 'as_needed'
      ELSE 'annual'
    END AS update_frequency,
    
    -- Metadata
    CURRENT_TIMESTAMP AS calculated_at,
    'int_valuation_assumptions' AS model_source
    
  FROM all_assumptions
)

SELECT * FROM final_assumptions
WHERE (company_id IS NULL OR company_id = 1)
  AND (portfolio_id IS NULL OR portfolio_id = 'micro-1')
ORDER BY assumption_category, assumption_subcategory, assumption_name