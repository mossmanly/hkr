{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['property_id'], 'unique': false},
      {'columns': ['portfolio_id'], 'unique': false}
    ]
  )
}}

-- Extract property valuation logic - streamlined version
-- References only staging models for clean data lineage

WITH base_properties AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    property_name,
    purchase_price,
    unit_count,
    avg_rent_per_unit,
    opex_ratio,
    gross_annual_income,
    price_per_unit
  FROM hkh_dev.stg_property_inputs
),

market_parameters AS (
  SELECT 
    MAX(CASE WHEN parameter_name = 'cap_rate_range_low' THEN parameter_value ELSE 0.06 END) AS market_cap_rate_low,
    MAX(CASE WHEN parameter_name = 'cap_rate_range_high' THEN parameter_value ELSE 0.08 END) AS market_cap_rate_high,
    MAX(CASE WHEN parameter_name = 'annual_rent_growth' THEN parameter_value ELSE 0.03 END) AS annual_rent_growth,
    MAX(CASE WHEN parameter_name = 'exit_cap_rate' THEN parameter_value ELSE 0.075 END) AS exit_cap_rate
  FROM hkh_dev.stg_market_parameters
),

-- Property valuation calculations
property_valuations AS (
  SELECT 
    bp.*,
    mp.market_cap_rate_low,
    mp.market_cap_rate_high,
    mp.annual_rent_growth,
    mp.exit_cap_rate,
    
    -- Current NOI for valuation
    bp.gross_annual_income * (1 - bp.opex_ratio) AS current_noi,
    
    -- Valuation using income approach (Cap Rate)
    CASE 
      WHEN mp.market_cap_rate_low > 0 THEN
        (bp.gross_annual_income * (1 - bp.opex_ratio)) / mp.market_cap_rate_low
      ELSE NULL
    END AS value_at_low_cap_rate,
    
    CASE 
      WHEN mp.market_cap_rate_high > 0 THEN
        (bp.gross_annual_income * (1 - bp.opex_ratio)) / mp.market_cap_rate_high
      ELSE NULL
    END AS value_at_high_cap_rate,
    
    -- Average market value
    CASE 
      WHEN mp.market_cap_rate_low > 0 AND mp.market_cap_rate_high > 0 THEN
        (bp.gross_annual_income * (1 - bp.opex_ratio)) / 
        ((mp.market_cap_rate_low + mp.market_cap_rate_high) / 2)
      ELSE NULL
    END AS market_value_mid_point,
    
    -- Purchase cap rate
    CASE 
      WHEN bp.purchase_price > 0 THEN
        (bp.gross_annual_income * (1 - bp.opex_ratio)) / bp.purchase_price
      ELSE NULL
    END AS purchase_cap_rate,
    
    -- Market positioning
    CASE 
      WHEN bp.purchase_price > 0 AND mp.market_cap_rate_low > 0 AND mp.market_cap_rate_high > 0 THEN
        CASE 
          WHEN (bp.gross_annual_income * (1 - bp.opex_ratio)) / bp.purchase_price > mp.market_cap_rate_high 
            THEN 'below_market_purchase'
          WHEN (bp.gross_annual_income * (1 - bp.opex_ratio)) / bp.purchase_price < mp.market_cap_rate_low 
            THEN 'above_market_purchase'
          ELSE 'market_rate_purchase'
        END
      ELSE 'unknown'
    END AS market_positioning,
    
    -- Value variance from purchase price
    CASE 
      WHEN bp.purchase_price > 0 AND mp.market_cap_rate_low > 0 AND mp.market_cap_rate_high > 0 THEN
        (((bp.gross_annual_income * (1 - bp.opex_ratio)) / 
          ((mp.market_cap_rate_low + mp.market_cap_rate_high) / 2)) - bp.purchase_price) / bp.purchase_price
      ELSE NULL
    END AS income_approach_variance_from_purchase,
    
    -- Investment quality scoring
    CASE 
      WHEN bp.purchase_price > 0 AND bp.gross_annual_income > 0 THEN
        CASE 
          WHEN (bp.gross_annual_income * (1 - bp.opex_ratio)) / bp.purchase_price >= 0.08 
            THEN 'high_quality'
          WHEN (bp.gross_annual_income * (1 - bp.opex_ratio)) / bp.purchase_price >= 0.06 
            THEN 'medium_quality'
          ELSE 'low_quality'
        END
      ELSE 'unrated'
    END AS investment_quality_rating,
    
    -- Metadata
    CURRENT_TIMESTAMP AS calculated_at,
    'int_property_valuations' AS model_source
    
  FROM base_properties bp
  CROSS JOIN market_parameters mp
)

SELECT * FROM property_valuations
WHERE company_id = 1 
  AND portfolio_id = 'micro-1'