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
        ROUND(SUM(pcf.egi), 0) AS total_gross_income,
        ROUND(SUM(pcf.noi), 0) AS total_noi,
        ROUND(SUM(pcf.opex), 0) AS total_operating_expenses,
        
        -- Cash flow projections using ACTUAL column names
        ROUND(SUM(pcf.btcf), 0) AS total_cf_before_capex,
        ROUND(SUM(pcf.capex), 0) AS total_capex_spending,
        ROUND(SUM(pcf.atcf_operations), 0) AS total_cf_after_capex,
        ROUND(SUM(pcf.capex_float_income), 0) AS total_capex_float_income

    FROM hkh_dev.int_property_cash_flows pcf
    WHERE pcf.company_id = 1
      AND pcf.year = 1  -- First year only for summary
    GROUP BY pcf.company_id, pcf.portfolio_id
),

-- Get property basics from staging for investment amounts
property_basics AS (
    SELECT 
        company_id,
        portfolio_id,
        COUNT(*) AS total_properties,
        SUM(unit_count) AS total_units,
        ROUND(SUM(purchase_price), 0) AS total_investment,
        ROUND(AVG(purchase_price), 0) AS avg_property_value,
        ROUND(AVG(unit_count), 0) AS avg_units_per_property,
        ROUND(AVG(avg_rent_per_unit), 0) AS avg_rent_per_unit,
        ROUND(AVG(opex_ratio), 4) AS avg_opex_ratio,
        ROUND(AVG(vacancy_rate), 4) AS avg_vacancy_rate
    FROM hkh_dev.stg_property_inputs
    WHERE company_id = 1
    GROUP BY company_id, portfolio_id
),

portfolio_financing AS (
    SELECT 
        ls.company_id,
        ls.portfolio_id,
        
        -- Debt aggregates using ACTUAL column names
        ROUND(SUM(ls.starting_balance), 0) AS total_debt,
        ROUND(AVG(ls.active_rate), 2) AS avg_interest_rate,
        ROUND(SUM(ls.annual_payment), 0) AS total_first_year_debt_service,
        ROUND(SUM(ls.interest_payment), 0) AS total_first_year_interest,
        ROUND(SUM(ls.principal_payment), 0) AS total_first_year_principal

    FROM hkh_dev.int_loan_schedules ls
    WHERE ls.company_id = 1
        AND ls.year = 1  -- First year only
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

    FROM hkh_dev.int_fee_calculations fc
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

    FROM hkh_dev.stg_portfolio_settings ps
    WHERE ps.company_id = 1
)

SELECT 
    -- Portfolio identification
    pb.company_id,
    pb.portfolio_id,
    ps.portfolio_name,
    ps.portfolio_description,
    ps.investment_strategy_clean,
    ps.is_default,
    ps.portfolio_status,
    
    -- Portfolio composition
    pb.total_properties,
    pb.total_units,
    pb.avg_property_value,
    pb.avg_units_per_property,
    ROUND(pb.total_investment / pb.total_units, 0) AS investment_per_unit,
    
    -- Investment summary
    pb.total_investment,
    ROUND(pb.total_investment - COALESCE(pf.total_debt, 0), 0) AS total_equity_invested,
    COALESCE(pf.total_debt, 0) AS total_debt,
    ROUND(COALESCE(pf.total_debt, 0) / pb.total_investment * 100, 2) AS portfolio_leverage_ratio,
    COALESCE(pf.avg_interest_rate, 0) AS avg_interest_rate,
    
    -- Income performance
    pp.total_gross_income,
    pp.total_operating_expenses,
    pp.total_noi,
    ROUND(pp.total_gross_income / pb.total_units, 0) AS income_per_unit,
    ROUND(pp.total_noi / pb.total_properties, 0) AS avg_noi_per_property,
    ROUND(pp.total_noi / pb.total_investment * 100, 2) AS portfolio_cap_rate,
    ROUND((pp.total_noi - COALESCE(fees.total_annual_fees, 0)) / pb.total_investment * 100, 2) AS net_yield_after_fees,
    
    -- Operating metrics
    pb.avg_opex_ratio,
    pb.avg_vacancy_rate,
    
    -- Returns analysis
    ROUND((pp.total_cf_after_capex - COALESCE(fees.total_annual_fees, 0)) / (pb.total_investment - COALESCE(pf.total_debt, 0)) * 100, 2) AS equity_cash_on_cash,
    
    -- Cash flow projections
    pp.total_cf_before_capex,
    pp.total_capex_spending AS total_capex_reserve,
    pp.total_cf_after_capex,
    pp.total_capex_float_income,
    
    -- Fee structure
    COALESCE(fees.total_acquisition_fees, 0) AS total_acquisition_fees,
    COALESCE(fees.total_mgmt_fees, 0) AS total_mgmt_fees,
    COALESCE(fees.total_disposition_fees, 0) AS total_disposition_fees,
    COALESCE(fees.total_annual_fees, 0) AS total_annual_fees,
    COALESCE(fees.avg_mgmt_fee_rate, 0) AS avg_mgmt_fee_rate,
    COALESCE(fees.avg_mgmt_fee_per_unit, 0) AS avg_mgmt_fee_per_unit,
    ROUND(COALESCE(fees.total_annual_fees, 0) / pb.total_properties, 0) AS avg_fees_per_property,
    
    -- Debt service
    COALESCE(pf.total_first_year_debt_service, 0) AS total_first_year_debt_service,
    COALESCE(pf.total_first_year_interest, 0) AS total_first_year_interest,
    COALESCE(pf.total_first_year_principal, 0) AS total_first_year_principal,
    
    -- Rent analysis
    pb.avg_rent_per_unit,
    ROUND(pp.total_gross_income / (pb.total_units * 12), 0) AS avg_monthly_rent_per_unit,
    
    -- Performance classifications for Metabase
    CASE 
        WHEN (pp.total_noi / pb.total_investment * 100) >= 8 THEN 'High Yield (8%+)'
        WHEN (pp.total_noi / pb.total_investment * 100) >= 6 THEN 'Medium Yield (6-8%)'
        ELSE 'Growth Focus (<6%)'
    END AS yield_category,
    
    CASE 
        WHEN pb.total_properties >= 20 THEN 'Large Portfolio (20+ properties)'
        WHEN pb.total_properties >= 10 THEN 'Medium Portfolio (10-19 properties)'
        WHEN pb.total_properties >= 5 THEN 'Small Portfolio (5-9 properties)'
        ELSE 'Micro Portfolio (1-4 properties)'
    END AS portfolio_size_category,
    
    -- Efficiency ratios for operational analysis
    ROUND(COALESCE(fees.total_annual_fees, 0) / pp.total_noi * 100, 2) AS fee_to_noi_ratio,
    ROUND(COALESCE(pf.total_first_year_debt_service, 0) / pp.total_noi * 100, 2) AS debt_service_to_noi_ratio,
    
    -- Key ratios for executive dashboard
    ROUND(pb.total_investment / 1000000, 1) AS total_investment_millions,
    ROUND(pp.total_noi / 1000, 0) AS total_noi_thousands,
    ROUND(pp.total_cf_after_capex / 1000, 0) AS annual_cashflow_thousands

FROM property_basics pb
LEFT JOIN portfolio_properties pp ON pb.company_id = pp.company_id 
    AND pb.portfolio_id = pp.portfolio_id
LEFT JOIN portfolio_financing pf ON pb.company_id = pf.company_id 
    AND pb.portfolio_id = pf.portfolio_id
LEFT JOIN portfolio_fees fees ON pb.company_id = fees.company_id 
    AND pb.portfolio_id = fees.portfolio_id
LEFT JOIN portfolio_settings ps ON pb.company_id = ps.company_id 
    AND pb.portfolio_id = ps.portfolio_id

WHERE pb.company_id = 1

ORDER BY pb.portfolio_id