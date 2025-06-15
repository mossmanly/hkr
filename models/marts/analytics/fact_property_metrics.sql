-- marts/analytics/fact_property_metrics.sql
-- Property investment metrics model for Metabase dashboards
-- Uses ACTUAL column names from intermediate models

{{ config(materialized='table') }}

WITH portfolio_properties AS (
    SELECT 
        pcf.company_id,
        pcf.portfolio_id,
        
        -- Portfolio composition using ACTUAL column names
        COUNT(DISTINCT pcf.property_id) AS total_properties,
        SUM(pcf.unit_count) AS total_units,
        ROUND(SUM(pcf.purchase_price), 0) AS total_investment,
        
        -- Revenue metrics using ACTUAL column names
        ROUND(SUM(pcf.gross_annual_income), 0) AS total_gross_income,
        ROUND(SUM(pcf.annual_noi), 0) AS total_noi,
        ROUND(SUM(pcf.annual_operating_expenses), 0) AS total_operating_expenses,
        
        -- Performance aggregates using ACTUAL column names
        ROUND(AVG(pcf.cap_rate), 2) AS avg_cap_rate,
        ROUND(AVG(pcf.cash_on_cash_return), 2) AS avg_cash_on_cash,
        ROUND(SUM(pcf.annual_noi) / SUM(pcf.purchase_price) * 100, 2) AS portfolio_cap_rate,
        
        -- Cash flow projections using ACTUAL column names
        ROUND(SUM(pcf.annual_cash_flow_before_capex), 0) AS total_cf_before_capex,
        ROUND(SUM(pcf.annual_capex_reserve), 0) AS total_capex_reserve,
        ROUND(SUM(pcf.annual_cash_flow_after_capex), 0) AS total_cf_after_capex,
        ROUND(SUM(pcf.equity_investment), 0) AS total_equity_invested,
        
        -- Property mix analysis using ACTUAL column names
        ROUND(AVG(pcf.purchase_price), 0) AS avg_property_value,
        ROUND(AVG(pcf.unit_count), 0) AS avg_units_per_property,
        ROUND(AVG(pcf.avg_rent_per_unit), 0) AS avg_rent_per_unit,
        ROUND(AVG(pcf.opex_ratio), 4) AS avg_opex_ratio,
        ROUND(AVG(pcf.vacancy_rate), 4) AS avg_vacancy_rate

    FROM {{ ref('int_property_cash_flows') }} pcf
    WHERE pcf.company_id = 1
    GROUP BY pcf.company_id, pcf.portfolio_id
),

portfolio_financing AS (
    SELECT 
        ls.company_id,
        ls.portfolio_id,
        
        -- Debt aggregates using ACTUAL column names
        ROUND(SUM(DISTINCT ls.loan_amount), 0) AS total_debt,
        ROUND(AVG(ls.ds_ltv), 2) AS avg_ltv,
        ROUND(AVG(ls.ds_int), 2) AS avg_interest_rate,
        ROUND(SUM(ls.first_year_payments), 0) AS total_first_year_debt_service,
        ROUND(SUM(ls.first_year_interest), 0) AS total_first_year_interest,
        ROUND(SUM(ls.first_year_principal), 0) AS total_first_year_principal

    FROM {{ ref('int_loan_schedules') }} ls
    WHERE ls.company_id = 1
        AND ls.is_first_year = true
    GROUP BY ls.company_id, ls.portfolio_id
),

portfolio_fees AS (
    SELECT 
        fc.company_id,
        fc.portfolio_id,
        
        -- Fee totals using ACTUAL column names
        ROUND(SUM(fc.acquisition_fee), 0) AS total_acquisition_fees,
        ROUND(SUM(fc.annual_management_fee), 0) AS total_mgmt_fees,
        ROUND(SUM(fc.estimated_disposition_fee), 0) AS total_disposition_fees,
        ROUND(SUM(fc.total_annual_fees), 0) AS total_annual_fees,
        
        -- Fee analysis using ACTUAL column names
        ROUND(AVG(fc.management_fee_rate), 2) AS avg_mgmt_fee_rate,
        ROUND(AVG(fc.management_fee_per_unit), 0) AS avg_mgmt_fee_per_unit

    FROM {{ ref('int_fee_calculations') }} fc
    WHERE fc.company_id = 1
    GROUP BY fc.company_id, fc.portfolio_id
),

portfolio_settings AS (
    SELECT 
        ps.company_id,
        ps.portfolio_id,
        ps.portfolio_name,
        ps.portfolio_description,
        ps.investment_strategy_clean,
        ps.is_default,
        ps.portfolio_status

    FROM {{ ref('stg_portfolio_settings') }} ps
    WHERE ps.company_id = 1
)

SELECT 
    -- Portfolio identification
    pp.company_id,
    pp.portfolio_id,
    ps.portfolio_name,
    ps.portfolio_description,
    ps.investment_strategy_clean,
    ps.is_default,
    ps.portfolio_status,
    
    -- Portfolio composition
    pp.total_properties,
    pp.total_units,
    pp.avg_property_value,
    pp.avg_units_per_property,
    ROUND(pp.total_investment / pp.total_units, 0) AS investment_per_unit,
    
    -- Investment summary
    pp.total_investment,
    pp.total_equity_invested,
    COALESCE(pf.total_debt, 0) AS total_debt,
    ROUND(COALESCE(pf.total_debt, 0) / pp.total_investment * 100, 2) AS portfolio_leverage_ratio,
    COALESCE(pf.avg_ltv, 0) AS avg_ltv,
    COALESCE(pf.avg_interest_rate, 0) AS avg_interest_rate,
    
    -- Income performance
    pp.total_gross_income,
    pp.total_operating_expenses,
    pp.total_noi,
    ROUND(pp.total_gross_income / pp.total_units, 0) AS income_per_unit,
    ROUND(pp.total_noi / pp.total_properties, 0) AS avg_noi_per_property,
    pp.portfolio_cap_rate,
    ROUND((pp.total_noi - COALESCE(fees.total_annual_fees, 0)) / pp.total_investment * 100, 2) AS net_yield_after_fees,
    
    -- Operating metrics
    pp.avg_opex_ratio,
    pp.avg_vacancy_rate,
    
    -- Returns analysis
    pp.avg_cash_on_cash,
    ROUND((pp.total_cf_after_capex - COALESCE(fees.total_annual_fees, 0)) / pp.total_equity_invested * 100, 2) AS equity_cash_on_cash,
    
    -- Cash flow projections
    pp.total_cf_before_capex,
    pp.total_capex_reserve,
    pp.total_cf_after_capex,
    
    -- Fee structure
    COALESCE(fees.total_acquisition_fees, 0) AS total_acquisition_fees,
    COALESCE(fees.total_mgmt_fees, 0) AS total_mgmt_fees,
    COALESCE(fees.total_disposition_fees, 0) AS total_disposition_fees,
    COALESCE(fees.total_annual_fees, 0) AS total_annual_fees,
    COALESCE(fees.avg_mgmt_fee_rate, 0) AS avg_mgmt_fee_rate,
    COALESCE(fees.avg_mgmt_fee_per_unit, 0) AS avg_mgmt_fee_per_unit,
    ROUND(COALESCE(fees.total_annual_fees, 0) / pp.total_properties, 0) AS avg_fees_per_property,
    
    -- Debt service
    COALESCE(pf.total_first_year_debt_service, 0) AS total_first_year_debt_service,
    COALESCE(pf.total_first_year_interest, 0) AS total_first_year_interest,
    COALESCE(pf.total_first_year_principal, 0) AS total_first_year_principal,
    
    -- Rent analysis
    pp.avg_rent_per_unit,
    ROUND(pp.total_gross_income / (pp.total_units * 12), 0) AS avg_monthly_rent_per_unit,
    
    -- Performance classifications for Metabase
    CASE 
        WHEN pp.portfolio_cap_rate >= 8 THEN 'High Yield (8%+)'
        WHEN pp.portfolio_cap_rate >= 6 THEN 'Medium Yield (6-8%)'
        ELSE 'Growth Focus (<6%)'
    END AS yield_category,
    
    CASE 
        WHEN pp.total_properties >= 20 THEN 'Large Portfolio (20+ properties)'
        WHEN pp.total_properties >= 10 THEN 'Medium Portfolio (10-19 properties)'
        WHEN pp.total_properties >= 5 THEN 'Small Portfolio (5-9 properties)'
        ELSE 'Micro Portfolio (1-4 properties)'
    END AS portfolio_size_category,
    
    -- Efficiency ratios for operational analysis
    ROUND(COALESCE(fees.total_annual_fees, 0) / pp.total_noi * 100, 2) AS fee_to_noi_ratio,
    ROUND(COALESCE(pf.total_first_year_debt_service, 0) / pp.total_noi * 100, 2) AS debt_service_to_noi_ratio,
    
    -- Key ratios for executive dashboard
    ROUND(pp.total_investment / 1000000, 1) AS total_investment_millions,
    ROUND(pp.total_noi / 1000, 0) AS total_noi_thousands,
    ROUND(pp.total_cf_after_capex / 1000, 0) AS annual_cashflow_thousands

FROM portfolio_properties pp
LEFT JOIN portfolio_financing pf ON pp.company_id = pf.company_id 
    AND pp.portfolio_id = pf.portfolio_id
LEFT JOIN portfolio_fees fees ON pp.company_id = fees.company_id 
    AND pp.portfolio_id = fees.portfolio_id
LEFT JOIN portfolio_settings ps ON pp.company_id = ps.company_id 
    AND pp.portfolio_id = ps.portfolio_id

WHERE pp.company_id = 1

ORDER BY pp.portfolio_id