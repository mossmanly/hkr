-- marts/analytics/fact_portfolio_summary.sql
-- Executive portfolio summary for high-level Metabase dashboards
-- Uses ACTUAL column names from intermediate models

{{ config(materialized='table') }}

WITH portfolio_properties AS (
    SELECT 
        pcf.company_id,
        pcf.portfolio_id,
        
        -- Portfolio composition (using ACTUAL column names) - ROUND DOLLAR AMOUNTS
        COUNT(DISTINCT pcf.property_id) AS total_properties,
        ROUND(SUM(pcf.egi), 0) AS total_gross_income,
        ROUND(SUM(pcf.noi), 0) AS total_noi,
        ROUND(SUM(pcf.opex), 0) AS total_operating_expenses,
        
        -- Performance aggregates - KEEP RATIO PRECISION
        ROUND(AVG(CASE WHEN pcf.egi > 0 THEN pcf.noi / pcf.egi ELSE 0 END), 2) AS avg_noi_margin,
        ROUND(SUM(pcf.atcf_operations), 0) AS total_cf_after_capex,
        
        -- Cash flow projections - ROUND DOLLAR AMOUNTS
        ROUND(SUM(pcf.btcf), 0) AS total_cf_before_capex,
        ROUND(SUM(pcf.capex), 0) AS total_capex_spending,
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
        
        -- Debt aggregates (using ACTUAL column names) - ROUND DOLLAR AMOUNTS
        ROUND(SUM(ls.starting_balance), 0) AS total_debt,
        ROUND(AVG(ls.active_rate), 2) AS avg_interest_rate,
        ROUND(SUM(ls.annual_payment), 0) AS total_first_year_debt_service,
        ROUND(SUM(ls.interest_payment), 0) AS total_first_year_interest,
        ROUND(SUM(ls.principal_payment), 0) AS total_first_year_principal

    FROM hkh_dev.int_loan_schedules ls
    WHERE ls.company_id = 1
        AND ls.year = 1  -- Only first year for annual summary
    GROUP BY ls.company_id, ls.portfolio_id
),

portfolio_fees AS (
    SELECT 
        fc.company_id,
        fc.portfolio_id,
        
        -- Fee totals (using ACTUAL column names) - ROUND DOLLAR AMOUNTS
        ROUND(SUM(fc.acquisition_fee), 0) AS total_acquisition_fees,
        ROUND(SUM(fc.annual_management_fee), 0) AS total_mgmt_fees,
        ROUND(SUM(fc.estimated_disposition_fee), 0) AS total_disposition_fees,
        ROUND(SUM(fc.total_annual_fees), 0) AS total_annual_fees,
        
        -- Fee analysis - KEEP RATIO PRECISION AND ROUND DOLLAR AMOUNTS
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
        ps.investment_strategy,
        ps.investment_strategy_clean,
        ps.is_default,
        ps.is_active,
        ps.created_at,
        ps.target_property_count,
        ps.target_total_units,
        ps.portfolio_status

    FROM hkh_dev.stg_portfolio_settings ps
    WHERE ps.company_id = 1
),

portfolio_analysis AS (
    SELECT 
        pb.*,
        pp.total_gross_income,
        pp.total_noi,
        pp.total_operating_expenses,
        pp.total_cf_before_capex,
        pp.total_cf_after_capex,
        pp.total_capex_spending,
        pp.total_capex_float_income,
        pf.total_debt,
        pf.avg_interest_rate,
        pf.total_first_year_debt_service,
        pf.total_first_year_interest,
        pf.total_first_year_principal,
        fees.total_acquisition_fees,
        fees.total_mgmt_fees,
        fees.total_disposition_fees,
        fees.total_annual_fees,
        fees.avg_mgmt_fee_rate,
        fees.avg_mgmt_fee_per_unit,
        ps.portfolio_name,
        ps.portfolio_description,
        ps.investment_strategy,
        ps.investment_strategy_clean,
        ps.is_default,
        ps.target_property_count,
        ps.target_total_units,
        ps.portfolio_status,
        
        -- Calculated metrics - ROUND DOLLAR AMOUNTS, KEEP RATIO PRECISION
        ROUND(pb.total_investment - COALESCE(pf.total_debt, 0), 0) AS total_equity_invested_calc,
        ROUND(COALESCE(pf.total_debt, 0) / pb.total_investment * 100, 2) AS portfolio_leverage_ratio,
        ROUND((pp.total_noi - COALESCE(fees.total_annual_fees, 0)) / pb.total_investment * 100, 2) AS net_yield_after_fees,
        ROUND((pp.total_cf_after_capex - COALESCE(fees.total_annual_fees, 0)) / (pb.total_investment - COALESCE(pf.total_debt, 0)) * 100, 2) AS equity_cash_on_cash,
        
        -- Portfolio efficiency metrics - ROUND DOLLAR AMOUNTS
        ROUND(pp.total_noi / pb.total_properties, 0) AS avg_noi_per_property,
        ROUND(COALESCE(fees.total_annual_fees, 0) / pb.total_properties, 0) AS avg_fees_per_property,
        ROUND(pb.total_investment / pb.total_units, 0) AS investment_per_unit,
        ROUND(pp.total_gross_income / pb.total_units, 0) AS income_per_unit

    FROM property_basics pb
    LEFT JOIN portfolio_properties pp ON pb.company_id = pp.company_id 
        AND pb.portfolio_id = pp.portfolio_id
    LEFT JOIN portfolio_financing pf ON pb.company_id = pf.company_id 
        AND pb.portfolio_id = pf.portfolio_id
    LEFT JOIN portfolio_fees fees ON pb.company_id = fees.company_id 
        AND pb.portfolio_id = fees.portfolio_id
    LEFT JOIN portfolio_settings ps ON pb.company_id = ps.company_id 
        AND pb.portfolio_id = ps.portfolio_id
)

SELECT 
    -- Portfolio identification
    pa.company_id,
    pa.portfolio_id,
    pa.portfolio_name,
    pa.portfolio_description,
    pa.investment_strategy,
    pa.investment_strategy_clean,
    pa.is_default,
    pa.portfolio_status,
    
    -- Portfolio composition - DOLLAR AMOUNTS ROUNDED
    pa.total_properties,
    pa.total_units,
    pa.avg_property_value,
    pa.avg_units_per_property,
    pa.investment_per_unit,
    
    -- Investment summary - DOLLAR AMOUNTS ROUNDED, RATIOS KEEP PRECISION
    pa.total_investment,
    pa.total_equity_invested_calc,
    pa.total_debt,
    pa.portfolio_leverage_ratio,
    pa.avg_interest_rate,
    
    -- Income performance - DOLLAR AMOUNTS ROUNDED, RATIOS KEEP PRECISION
    pa.total_gross_income,
    pa.total_operating_expenses,
    pa.total_noi,
    pa.income_per_unit,
    pa.avg_noi_per_property,
    ROUND(pa.total_noi / pa.total_investment * 100, 2) AS portfolio_cap_rate,
    pa.net_yield_after_fees,
    
    -- Operating metrics - KEEP RATIO PRECISION
    pa.avg_opex_ratio,
    pa.avg_vacancy_rate,
    
    -- Portfolio targets
    pa.target_property_count,
    pa.target_total_units,
    
    -- Returns analysis - KEEP RATIO PRECISION
    pa.equity_cash_on_cash,
    
    -- Cash flow projections - DOLLAR AMOUNTS ROUNDED
    pa.total_cf_before_capex,
    pa.total_capex_spending AS total_capex_reserve,
    pa.total_cf_after_capex,
    pa.total_capex_float_income,
    
    -- Fee structure - DOLLAR AMOUNTS ROUNDED, RATIOS KEEP PRECISION
    pa.total_acquisition_fees,
    pa.total_mgmt_fees,
    pa.total_disposition_fees,
    pa.total_annual_fees,
    pa.avg_mgmt_fee_rate,
    pa.avg_mgmt_fee_per_unit,
    pa.avg_fees_per_property,
    
    -- Debt service - DOLLAR AMOUNTS ROUNDED
    pa.total_first_year_debt_service,
    pa.total_first_year_interest,
    pa.total_first_year_principal,
    
    -- Rent analysis - ROUND DOLLAR AMOUNTS
    pa.avg_rent_per_unit,
    ROUND(pa.total_gross_income / (pa.total_units * 12), 0) AS avg_monthly_rent_per_unit,
    
    -- Performance classifications for Metabase
    CASE 
        WHEN (pa.total_noi / pa.total_investment * 100) >= 8 THEN 'High Yield (8%+)'
        WHEN (pa.total_noi / pa.total_investment * 100) >= 6 THEN 'Medium Yield (6-8%)'
        ELSE 'Growth Focus (<6%)'
    END AS yield_category,
    
    CASE 
        WHEN pa.equity_cash_on_cash >= 15 THEN 'Excellent Performance (15%+)'
        WHEN pa.equity_cash_on_cash >= 10 THEN 'Strong Performance (10-15%)'
        WHEN pa.equity_cash_on_cash >= 8 THEN 'Good Performance (8-10%)'
        ELSE 'Conservative Performance (<8%)'
    END AS performance_category,
    
    CASE 
        WHEN pa.portfolio_leverage_ratio >= 80 THEN 'High Leverage (80%+)'
        WHEN pa.portfolio_leverage_ratio >= 60 THEN 'Medium Leverage (60-80%)'
        WHEN pa.portfolio_leverage_ratio >= 40 THEN 'Conservative Leverage (40-60%)'
        ELSE 'Low Leverage (<40%)'
    END AS leverage_category,
    
    CASE 
        WHEN pa.total_properties >= 20 THEN 'Large Portfolio (20+ properties)'
        WHEN pa.total_properties >= 10 THEN 'Medium Portfolio (10-19 properties)'
        WHEN pa.total_properties >= 5 THEN 'Small Portfolio (5-9 properties)'
        ELSE 'Micro Portfolio (1-4 properties)'
    END AS portfolio_size_category,
    
    -- Efficiency ratios for operational analysis - KEEP RATIO PRECISION
    ROUND(pa.total_annual_fees / pa.total_noi * 100, 2) AS fee_to_noi_ratio,
    ROUND(pa.total_first_year_debt_service / pa.total_noi * 100, 2) AS debt_service_to_noi_ratio,
    ROUND((pa.total_noi - pa.total_annual_fees - pa.total_first_year_debt_service) / pa.total_noi * 100, 2) AS net_margin_after_all_costs,
    
    -- Key ratios for executive dashboard - ROUNDED FOR READABILITY
    ROUND(pa.total_investment / 1000000, 1) AS total_investment_millions,
    ROUND(pa.total_noi / 1000, 0) AS total_noi_thousands,
    ROUND(pa.total_cf_after_capex / 1000, 0) AS annual_cashflow_thousands

FROM portfolio_analysis pa
WHERE pa.company_id = 1  -- Portfolio architecture filtering

ORDER BY pa.portfolio_id