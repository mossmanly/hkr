{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['property_id'], 'unique': false},
      {'columns': ['portfolio_id'], 'unique': false}
    ]
  )
}}

-- Extract fee calculation logic from management_fee_calc
-- Handles acquisition fees, management fees, disposition fees, and other fee structures
-- References only staging models for clean data lineage

WITH base_properties AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    property_name,
    purchase_price,
    unit_count,
    gross_annual_income  -- This exists in staging!
  FROM hkh_dev.stg_property_inputs
),

portfolio_settings AS (
  SELECT 
    company_id,
    portfolio_id,
    portfolio_name,
    investment_strategy,
    0.08 AS management_fee_rate,  -- Default since column doesn't exist
    is_default,
    is_active
  FROM hkh_dev.stg_portfolio_settings
  WHERE is_active = TRUE
),

-- Fee calculation logic
fee_calculations AS (
  SELECT 
    bp.property_id,
    bp.company_id,
    bp.portfolio_id,
    bp.property_name,
    bp.purchase_price,
    bp.unit_count,
    bp.gross_annual_income,
    ps.portfolio_name,
    ps.investment_strategy,
    ps.management_fee_rate,
    
    -- Acquisition fee (typically 1-2% of purchase price)
    bp.purchase_price * 0.015 AS acquisition_fee, -- 1.5% default
    
    -- Annual management fee (based on portfolio settings or default)
    bp.gross_annual_income * COALESCE(ps.management_fee_rate, 0.08) AS annual_management_fee,
    
    -- Monthly management fee
    (bp.gross_annual_income * COALESCE(ps.management_fee_rate, 0.08)) / 12.0 AS monthly_management_fee,
    
    -- Disposition fee (typically 1-3% of sale price, using purchase price as proxy)
    bp.purchase_price * 0.02 AS estimated_disposition_fee, -- 2% default
    
    -- Asset management fee (typically 1-2% of gross revenue)
    bp.gross_annual_income * 0.015 AS annual_asset_management_fee,
    
    -- Property management fee (typically 8-12% of gross revenue)
    bp.gross_annual_income * 0.10 AS annual_property_management_fee,
    
    -- Leasing fee (typically 1 month rent per unit per year, amortized)
    (bp.gross_annual_income / bp.unit_count) * bp.unit_count * 0.08 AS annual_leasing_fee, -- 1 month rent / 12 months
    
    -- Maintenance and repair fee (typically 2-4% of gross revenue)
    bp.gross_annual_income * 0.03 AS annual_maintenance_fee,
    
    -- Total annual fees
    bp.purchase_price * 0.015 + -- Acquisition fee (amortized over hold period)
    bp.gross_annual_income * COALESCE(ps.management_fee_rate, 0.08) + -- Management fee
    bp.gross_annual_income * 0.015 + -- Asset management fee
    bp.gross_annual_income * 0.10 + -- Property management fee
    bp.gross_annual_income * 0.08 + -- Leasing fee
    bp.gross_annual_income * 0.03 AS total_annual_fees,
    
    -- Fee percentages for analysis
    CASE 
      WHEN bp.gross_annual_income > 0 THEN
        (bp.gross_annual_income * COALESCE(ps.management_fee_rate, 0.08) + 
         bp.gross_annual_income * 0.015 + 
         bp.gross_annual_income * 0.10 + 
         bp.gross_annual_income * 0.08 + 
         bp.gross_annual_income * 0.03) / bp.gross_annual_income
      ELSE 0
    END AS total_fee_percentage,
    
    -- Fee efficiency metrics
    CASE 
      WHEN bp.unit_count > 0 THEN
        (bp.gross_annual_income * COALESCE(ps.management_fee_rate, 0.08)) / bp.unit_count
      ELSE 0
    END AS management_fee_per_unit,
    
    CASE 
      WHEN bp.purchase_price > 0 THEN
        (bp.gross_annual_income * COALESCE(ps.management_fee_rate, 0.08)) / bp.purchase_price
      ELSE 0
    END AS management_fee_as_percent_of_value,
    
    -- Waterfall-related fees (promote, carried interest)
    CASE 
      WHEN ps.investment_strategy = 'value_add' THEN bp.gross_annual_income * 0.20 -- 20% promote
      WHEN ps.investment_strategy = 'opportunistic' THEN bp.gross_annual_income * 0.25 -- 25% promote
      ELSE bp.gross_annual_income * 0.15 -- 15% promote for core/core_plus
    END AS annual_promote_potential,
    
    -- Fee category flags
    CASE 
      WHEN COALESCE(ps.management_fee_rate, 0.08) > 0.10 THEN 'high_fee'
      WHEN COALESCE(ps.management_fee_rate, 0.08) < 0.06 THEN 'low_fee'
      ELSE 'standard_fee'
    END AS fee_category,
    
    -- Metadata
    CURRENT_TIMESTAMP AS calculated_at,
    'int_fee_calculations' AS model_source
    
  FROM base_properties bp
  LEFT JOIN portfolio_settings ps ON bp.company_id = ps.company_id 
                                 AND bp.portfolio_id = ps.portfolio_id
)

SELECT * FROM fee_calculations
WHERE company_id = 1 -- Preserve company scoping
  AND portfolio_id = 'micro-1' -- Preserve portfolio filtering