{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['property_id'], 'unique': false},
      {'columns': ['portfolio_id'], 'unique': false}
    ]
  )
}}

-- Extract property cash flow calculations - streamlined version
-- References only staging models to maintain clean data lineage

WITH base_properties AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    property_name,
    purchase_price,
    unit_count,
    avg_rent_per_unit,
    ds_ltv,
    ds_int,
    ds_term,
    opex_ratio,
    vacancy_rate,
    gross_annual_income,
    price_per_unit
  FROM {{ ref('stg_property_inputs') }}
),

-- Core cash flow calculations
property_cash_flows AS (
  SELECT 
    bp.*,
    
    -- Income calculations
    bp.gross_annual_income AS annual_gross_income,
    bp.gross_annual_income / 12.0 AS monthly_gross_income,
    
    -- Vacancy adjusted income
    bp.gross_annual_income * (1 - bp.vacancy_rate) AS effective_gross_income,
    
    -- Operating expense calculations
    bp.gross_annual_income * bp.opex_ratio AS annual_operating_expenses,
    
    -- Net Operating Income (NOI)
    bp.gross_annual_income * (1 - bp.vacancy_rate - bp.opex_ratio) AS annual_noi,
    
    -- Debt service calculations
    bp.purchase_price * bp.ds_ltv AS loan_amount,
    
    -- Calculate monthly debt service using standard mortgage formula
    CASE 
      WHEN bp.ds_int > 0 AND bp.ds_term > 0 THEN
        (bp.purchase_price * bp.ds_ltv) * 
        ((bp.ds_int/12) * POWER(1 + bp.ds_int/12, bp.ds_term * 12)) / 
        (POWER(1 + bp.ds_int/12, bp.ds_term * 12) - 1)
      ELSE 0
    END AS monthly_debt_service,
    
    -- Annual debt service
    CASE 
      WHEN bp.ds_int > 0 AND bp.ds_term > 0 THEN
        12 * (bp.purchase_price * bp.ds_ltv) * 
        ((bp.ds_int/12) * POWER(1 + bp.ds_int/12, bp.ds_term * 12)) / 
        (POWER(1 + bp.ds_int/12, bp.ds_term * 12) - 1)
      ELSE 0
    END AS annual_debt_service,
    
    -- Cash flow before CapEx
    (bp.gross_annual_income * (1 - bp.vacancy_rate - bp.opex_ratio)) - 
    CASE 
      WHEN bp.ds_int > 0 AND bp.ds_term > 0 THEN
        12 * (bp.purchase_price * bp.ds_ltv) * 
        ((bp.ds_int/12) * POWER(1 + bp.ds_int/12, bp.ds_term * 12)) / 
        (POWER(1 + bp.ds_int/12, bp.ds_term * 12) - 1)
      ELSE 0
    END AS annual_cash_flow_before_capex,
    
    -- CapEx reserve (5% of effective gross income)
    bp.gross_annual_income * (1 - bp.vacancy_rate) * 0.05 AS annual_capex_reserve,
    
    -- Final cash flow after reserves
    (bp.gross_annual_income * (1 - bp.vacancy_rate - bp.opex_ratio)) - 
    CASE 
      WHEN bp.ds_int > 0 AND bp.ds_term > 0 THEN
        12 * (bp.purchase_price * bp.ds_ltv) * 
        ((bp.ds_int/12) * POWER(1 + bp.ds_int/12, bp.ds_term * 12)) / 
        (POWER(1 + bp.ds_int/12, bp.ds_term * 12) - 1)
      ELSE 0
    END - (bp.gross_annual_income * (1 - bp.vacancy_rate) * 0.05) AS annual_cash_flow_after_capex,
    
    -- Cash-on-cash return
    CASE 
      WHEN bp.purchase_price * (1 - bp.ds_ltv) > 0 THEN
        ((bp.gross_annual_income * (1 - bp.vacancy_rate - bp.opex_ratio)) - 
         CASE 
           WHEN bp.ds_int > 0 AND bp.ds_term > 0 THEN
             12 * (bp.purchase_price * bp.ds_ltv) * 
             ((bp.ds_int/12) * POWER(1 + bp.ds_int/12, bp.ds_term * 12)) / 
             (POWER(1 + bp.ds_int/12, bp.ds_term * 12) - 1)
           ELSE 0
         END - (bp.gross_annual_income * (1 - bp.vacancy_rate) * 0.05)) / 
        (bp.purchase_price * (1 - bp.ds_ltv))
      ELSE 0
    END AS cash_on_cash_return,
    
    -- Equity investment
    bp.purchase_price * (1 - bp.ds_ltv) AS equity_investment,
    
    -- Cap rate
    CASE 
      WHEN bp.purchase_price > 0 THEN
        (bp.gross_annual_income * (1 - bp.vacancy_rate - bp.opex_ratio)) / bp.purchase_price
      ELSE 0
    END AS cap_rate,
    
    -- Debt service coverage ratio
    CASE 
      WHEN bp.ds_int > 0 AND bp.ds_term > 0 THEN
        (bp.gross_annual_income * (1 - bp.vacancy_rate - bp.opex_ratio)) / 
        (12 * (bp.purchase_price * bp.ds_ltv) * 
         ((bp.ds_int/12) * POWER(1 + bp.ds_int/12, bp.ds_term * 12)) / 
         (POWER(1 + bp.ds_int/12, bp.ds_term * 12) - 1))
      ELSE NULL
    END AS debt_service_coverage_ratio,
    
    -- Investment quality scoring
    CASE 
      WHEN (bp.gross_annual_income * (1 - bp.vacancy_rate - bp.opex_ratio)) / bp.purchase_price >= 0.08 
        THEN 'high_quality'
      WHEN (bp.gross_annual_income * (1 - bp.vacancy_rate - bp.opex_ratio)) / bp.purchase_price >= 0.06 
        THEN 'medium_quality'
      ELSE 'low_quality'
    END AS investment_quality_rating,
    
    -- Metadata
    CURRENT_TIMESTAMP AS calculated_at,
    'int_property_cash_flows' AS model_source
    
  FROM base_properties bp
)

SELECT * FROM property_cash_flows
WHERE company_id = 1 
  AND portfolio_id = 'micro-1'