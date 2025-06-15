{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['property_id'], 'unique': false},
      {'columns': ['year'], 'unique': false}
    ]
  )
}}

-- Rebuild the sophisticated CapEx reserve management system
-- CRITICAL: Interest earnings flow into cash flow calculations

WITH property_basics AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    unit_count,
    capex_per_unit,
    purchase_price,
    gross_annual_income
  FROM hkh_dev.stg_property_inputs
  WHERE company_id = 1
),

-- Get capex factors and rebate data
capex_factors_with_rebates AS (
  SELECT 
    property_id,
    year,
    capex_factor,
    spending_focus,
    rationale,
    COALESCE(heat_pump_rebate_per_unit, 0) AS heat_pump_rebate_per_unit,
    COALESCE(efficiency_rebate_per_unit, 0) AS efficiency_rebate_per_unit,
    COALESCE(solar_rebate_per_unit, 0) AS solar_rebate_per_unit
  FROM hkh_dev.stg_capex_factors
),

-- Apply factors to get annual budgets
annual_capex_by_period AS (
  SELECT 
    pb.property_id,
    pb.company_id,
    pb.portfolio_id,
    cf.year,
    pb.unit_count,
    pb.capex_per_unit,
    cf.capex_factor,
    cf.spending_focus,
    
    -- Core formula: unit_count * capex_per_unit * capex_factor
    ROUND((pb.unit_count * pb.capex_per_unit * cf.capex_factor)::numeric, 0) AS annual_capex_budget,
    
    -- Total rebate potential per unit
    (cf.heat_pump_rebate_per_unit + cf.efficiency_rebate_per_unit + cf.solar_rebate_per_unit) AS total_rebate_per_unit,
    
    -- Total rebate potential for property
    pb.unit_count * (cf.heat_pump_rebate_per_unit + cf.efficiency_rebate_per_unit + cf.solar_rebate_per_unit) AS total_rebate_potential
    
  FROM property_basics pb
  INNER JOIN capex_factors_with_rebates cf ON pb.property_id = cf.property_id
),

-- Project actual spending patterns based on focus ("Wild Ass Guesses")
capex_spending_projections AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    year,
    unit_count,
    capex_per_unit,
    capex_factor,
    spending_focus,
    annual_capex_budget,
    total_rebate_per_unit,
    total_rebate_potential,
    
    -- Project actual spending based on spending focus patterns
    CASE 
      WHEN spending_focus IN ('Turn Renovations', 'Roofing + Final Snap Renos', 'Flooring + Interior', 'Exterior + Structural') 
        THEN ROUND((annual_capex_budget * 1.25)::numeric, 0)  -- Major spending: 1.25x budget
      WHEN spending_focus IN ('Major Systems', 'HVAC Systems + Maintenance') 
        THEN ROUND((annual_capex_budget * 2.0)::numeric, 0)   -- Cyclical heavy: 2.0x budget every 5-6 years  
      WHEN spending_focus = 'Energy Efficiency' 
        THEN ROUND((annual_capex_budget * 0.95)::numeric, 0)  -- Consistent: 0.95x budget
      WHEN spending_focus IN ('Snap Renos + Emergency', 'Preventive Maintenance', 'Snap Renos + Maintenance') 
        THEN ROUND((annual_capex_budget * 0.6)::numeric, 0)   -- Steady lower: 0.6x budget
      ELSE annual_capex_budget  -- Default to budget
    END AS projected_capex_spent,
    
    -- Reserves set aside (slightly more than budget for big projects)
    ROUND((annual_capex_budget * 1.1)::numeric, 0) AS annual_reserves_set_aside
    
  FROM annual_capex_by_period
),

-- Calculate running balances and CRITICAL interest earnings
capex_cash_flows AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    year,
    annual_capex_budget,
    projected_capex_spent,
    annual_reserves_set_aside,
    total_rebate_potential,
    spending_focus,
    
    -- Running cumulative reserves
    SUM(annual_reserves_set_aside) OVER (
      PARTITION BY property_id 
      ORDER BY year 
      ROWS UNBOUNDED PRECEDING
    ) AS cumulative_reserves_raised,
    
    -- Running cumulative spending
    SUM(projected_capex_spent) OVER (
      PARTITION BY property_id 
      ORDER BY year 
      ROWS UNBOUNDED PRECEDING
    ) AS cumulative_capex_spent,
    
    -- Available cash for projects
    SUM(annual_reserves_set_aside) OVER (
      PARTITION BY property_id 
      ORDER BY year 
      ROWS UNBOUNDED PRECEDING
    ) - SUM(projected_capex_spent) OVER (
      PARTITION BY property_id 
      ORDER BY year 
      ROWS UNBOUNDED PRECEDING
    ) AS available_for_capex,
    
    -- CRITICAL: Interest earnings on reserves (2% annually)
    ROUND((
      SUM(annual_reserves_set_aside) OVER (
        PARTITION BY property_id 
        ORDER BY year 
        ROWS UNBOUNDED PRECEDING
      ) * 0.02
    )::numeric, 0) AS annual_interest_on_reserves
    
  FROM capex_spending_projections
),

-- Final business classifications and metrics
final_capex_management AS (
  SELECT 
    *,
    
    -- Business classifications for cash position
    CASE 
      WHEN available_for_capex < 0 THEN 'deficit'
      WHEN available_for_capex < annual_capex_budget THEN 'low'
      WHEN available_for_capex < (annual_capex_budget * 2) THEN 'balanced'
      ELSE 'excess'
    END AS cash_position_category,
    
    -- Reserve efficiency ratio
    CASE 
      WHEN cumulative_reserves_raised > 0 THEN 
        ROUND((cumulative_capex_spent::numeric / cumulative_reserves_raised::numeric), 3)
      ELSE 0
    END AS reserve_utilization_ratio,
    
    -- Interest yield as % of annual budget
    CASE 
      WHEN annual_capex_budget > 0 THEN 
        ROUND((annual_interest_on_reserves::numeric / annual_capex_budget::numeric) * 100, 1)
      ELSE 0
    END AS interest_yield_pct_of_budget,
    
    -- Ending reserve balance (for monitoring)
    available_for_capex AS ending_reserve_balance,
    
    -- Spending efficiency (actual vs budget)
    CASE 
      WHEN annual_capex_budget > 0 THEN 
        ROUND((projected_capex_spent::numeric / annual_capex_budget::numeric), 2)
      ELSE 0
    END AS spending_efficiency_ratio
    
  FROM capex_cash_flows
)

SELECT 
  property_id,
  company_id,
  portfolio_id,
  year,
  spending_focus,
  
  -- Core calculations
  annual_capex_budget,
  projected_capex_spent AS capex_spent,  -- Match fact_capex_with_incentives expectation
  annual_reserves_set_aside,
  
  -- Running balances
  cumulative_reserves_raised,
  cumulative_capex_spent,
  available_for_capex,
  ending_reserve_balance,
  
  -- CRITICAL: Interest earnings for cash flow integration
  annual_interest_on_reserves AS interest_income,  -- Match original model field name
  
  -- Incentive data
  total_rebate_potential,
  
  -- Business metrics
  cash_position_category,
  reserve_utilization_ratio,
  interest_yield_pct_of_budget,
  spending_efficiency_ratio,
  
  -- Metadata
  CURRENT_TIMESTAMP AS calculated_at,
  'int_capex_reserves' AS model_source
  
FROM final_capex_management
ORDER BY property_id, year