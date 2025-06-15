-- marts/finance/fact_property_performance.sql
-- Primary property performance model for Metabase dashboards
-- Portfolio filtering with company scoping preserved

{{ config(materialized='table') }}

SELECT
    -- Primary Keys
    pcf.property_id,
    pcf.company_id,
    pcf.portfolio_id,
    
    -- Property Information
    pcf.property_name,
    pcf.property_address,
    pcf.property_city,
    pcf.property_state,
    pcf.property_zip,
    
    -- Property Fundamentals (rounded to whole dollars)
    ROUND(pcf.purchase_price, 0) AS purchase_price,
    pcf.unit_count,
    ROUND(pcf.avg_rent_per_unit, 0) AS avg_rent_per_unit,
    ROUND(pcf.annual_noi, 0) AS annual_noi,
    ROUND(pcf.gross_annual_income, 0) AS gross_annual_income,
    
    -- Financial Structure
    pcf.ds_ltv,
    pcf.ds_int,
    ROUND(pcf.debt_service_coverage_ratio, 2) AS debt_service_coverage_ratio,
    ROUND(pcf.equity_investment, 0) AS equity_investment,
    
    -- Performance Metrics
    ROUND(pcf.annual_cash_flow_before_capex, 0) AS annual_cash_flow_before_capex,
    ROUND(pcf.annual_cash_flow_after_capex, 0) AS annual_cash_flow_after_capex,
    ROUND(pcf.cash_on_cash_return, 4) AS cash_on_cash_return,
    ROUND(pcf.cap_rate, 4) AS cap_rate,
    
    -- Business Classifications
    pcf.property_size_category,
    pcf.cap_rate_category,
    pcf.return_category,
    
    -- Fee Information from intermediate model
    ROUND(fc.annual_management_fee, 0) AS annual_management_fee,
    ROUND(fc.acquisition_fee, 0) AS acquisition_fee,
    ROUND(fc.estimated_disposition_fee, 0) AS estimated_disposition_fee,
    ROUND(fc.total_annual_fees, 0) AS total_annual_fees,
    ROUND(fc.management_fee_per_unit, 0) AS management_fee_per_unit,
    fc.fee_category,
    
    -- Loan Information
    ROUND(ls.loan_amount, 0) AS loan_amount,
    ROUND(ls.monthly_payment, 0) AS monthly_payment,
    ROUND(ls.first_year_payments, 0) AS first_year_debt_service,
    ROUND(ls.first_year_interest, 0) AS first_year_interest,
    ROUND(ls.first_year_principal, 0) AS first_year_principal,
    
    -- Market Assumptions
    va.market_cap_rate,
    va.market_rent_growth,
    va.market_expense_growth,
    va.market_appreciation_rate

FROM {{ ref('int_property_cash_flows') }} pcf

-- Join fee calculations
LEFT JOIN {{ ref('int_fee_calculations') }} fc
    ON pcf.property_id = fc.property_id
    AND pcf.company_id = fc.company_id
    AND pcf.portfolio_id = fc.portfolio_id

-- Join loan schedules  
LEFT JOIN {{ ref('int_loan_schedules') }} ls
    ON pcf.property_id = ls.property_id
    AND pcf.company_id = ls.company_id
    AND pcf.portfolio_id = ls.portfolio_id

-- Join valuation assumptions
LEFT JOIN {{ ref('int_valuation_assumptions') }} va
    ON pcf.company_id = va.company_id
    AND pcf.portfolio_id = va.portfolio_id

-- Portfolio filtering: Only include default portfolio of this company
WHERE pcf.company_id = 1

ORDER BY pcf.property_id