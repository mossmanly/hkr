-- marts/finance/fact_property_analytics.sql
-- Property investment analytics and KPIs for Metabase dashboards
-- Uses ACTUAL column names from intermediate models

{{ config(materialized='table') }}

WITH property_fundamentals AS (
    SELECT 
        pcf.property_id,
        pcf.company_id,
        pcf.portfolio_id,
        pcf.property_name,
        
        -- Core property data (using ACTUAL column names) - ROUND DOLLAR AMOUNTS
        ROUND(pcf.purchase_price, 0) AS purchase_price,
        pcf.unit_count,
        ROUND(pcf.avg_rent_per_unit, 0) AS avg_rent_per_unit,
        ROUND(pcf.gross_annual_income, 0) AS gross_annual_income,
        ROUND(pcf.annual_gross_income, 0) AS annual_gross_income,
        ROUND(pcf.monthly_gross_income, 0) AS monthly_gross_income,
        ROUND(pcf.effective_gross_income, 0) AS effective_gross_income,
        pcf.opex_ratio,
        pcf.vacancy_rate,
        ROUND(pcf.annual_operating_expenses, 0) AS annual_operating_expenses,
        ROUND(pcf.annual_noi, 0) AS annual_noi,
        
        -- Financial structure
        pcf.ds_ltv,
        pcf.ds_int,
        pcf.ds_term,
        pcf.cash_on_cash_return,
        pcf.cap_rate,
        pcf.debt_service_coverage_ratio,
        ROUND(pcf.equity_investment, 0) AS equity_investment,
        
        -- Performance projections - ROUND DOLLAR AMOUNTS
        ROUND(pcf.loan_amount, 0) AS loan_amount,
        ROUND(pcf.monthly_debt_service, 0) AS monthly_debt_service,
        ROUND(pcf.annual_debt_service, 0) AS annual_debt_service,
        ROUND(pcf.annual_cash_flow_before_capex, 0) AS annual_cash_flow_before_capex,
        ROUND(pcf.annual_capex_reserve, 0) AS annual_capex_reserve,
        ROUND(pcf.annual_cash_flow_after_capex, 0) AS annual_cash_flow_after_capex,
        ROUND(pcf.price_per_unit, 0) AS price_per_unit,
        pcf.investment_quality_rating

    FROM {{ ref('int_property_cash_flows') }} pcf
    WHERE pcf.company_id = 1
),

market_assumptions AS (
    SELECT 
        va.company_id,
        va.portfolio_id,
        va.assumption_category,
        va.assumption_value,
        va.assumption_description,
        va.assumption_subcategory,
        va.assumption_risk_level

    FROM {{ ref('int_valuation_assumptions') }} va
    WHERE va.company_id = 1
),

property_fees AS (
    SELECT 
        fc.property_id,
        fc.company_id,
        fc.portfolio_id,
        ROUND(fc.acquisition_fee, 0) AS acquisition_fee,
        ROUND(fc.annual_management_fee, 0) AS annual_management_fee,
        ROUND(fc.estimated_disposition_fee, 0) AS estimated_disposition_fee,
        ROUND(fc.total_annual_fees, 0) AS total_annual_fees,
        fc.management_fee_rate,
        ROUND(fc.management_fee_per_unit, 0) AS management_fee_per_unit,
        fc.fee_category

    FROM {{ ref('int_fee_calculations') }} fc
    WHERE fc.company_id = 1
),

calculated_metrics AS (
    SELECT 
        pf.*,
        fees.acquisition_fee,
        fees.annual_management_fee,
        fees.estimated_disposition_fee,
        fees.total_annual_fees,
        fees.management_fee_rate,
        fees.management_fee_per_unit,
        fees.fee_category,
        
        -- Unit economics - ROUND DOLLAR AMOUNTS
        ROUND(pf.avg_rent_per_unit * 12, 0) AS annual_rent_per_unit,
        ROUND((pf.avg_rent_per_unit * 12) / NULLIF(pf.price_per_unit, 0) * 100, 2) AS gross_yield_percent,
        
        -- Cash flow metrics - ROUND DOLLAR AMOUNTS
        ROUND(pf.annual_noi - COALESCE(fees.total_annual_fees, 0), 0) AS noi_after_fees,
        ROUND((pf.annual_noi - COALESCE(fees.total_annual_fees, 0)) / pf.purchase_price * 100, 2) AS net_yield_after_fees,
        
        -- Performance ratios - KEEP RATIO PRECISION
        ROUND(pf.gross_annual_income / pf.purchase_price * 100, 2) AS gross_rent_multiplier,
        ROUND(pf.purchase_price / pf.gross_annual_income, 1) AS price_to_income_ratio,
        
        -- Efficiency metrics - ROUND DOLLAR AMOUNTS
        ROUND(pf.annual_noi / pf.unit_count, 0) AS noi_per_unit,
        ROUND(pf.annual_cash_flow_after_capex / pf.unit_count, 0) AS cash_flow_per_unit

    FROM property_fundamentals pf
    LEFT JOIN property_fees fees ON pf.property_id = fees.property_id 
        AND pf.company_id = fees.company_id 
        AND pf.portfolio_id = fees.portfolio_id
),

investment_analysis AS (
    SELECT 
        cm.*,
        
        -- Cash flow coverage ratios - KEEP RATIO PRECISION
        ROUND(cm.annual_noi / NULLIF(cm.total_annual_fees, 0), 2) AS noi_to_fees_ratio,
        ROUND(cm.annual_cash_flow_after_capex / NULLIF(cm.annual_management_fee, 0), 2) AS cash_flow_to_mgmt_fee_ratio,
        
        -- Risk metrics
        CASE 
            WHEN cm.cash_on_cash_return >= 12 THEN 'Low Risk (High Return)'
            WHEN cm.cash_on_cash_return >= 8 THEN 'Medium Risk'
            WHEN cm.cash_on_cash_return >= 5 THEN 'Higher Risk (Low Return)'
            ELSE 'High Risk'
        END AS risk_assessment,
        
        -- Market position
        CASE 
            WHEN cm.cap_rate >= 8 THEN 'Value Play (High Cap)'
            WHEN cm.cap_rate >= 6 THEN 'Balanced Investment'
            ELSE 'Growth Play (Low Cap)'
        END AS investment_strategy,
        
        -- Debt coverage analysis - KEEP RATIO PRECISION
        ROUND(cm.annual_noi / NULLIF(cm.annual_debt_service, 0), 2) AS debt_service_coverage_calc

    FROM calculated_metrics cm
)

SELECT 
    -- Property identification
    ia.property_id,
    ia.company_id,
    ia.portfolio_id,
    ia.property_name,
    
    -- Property fundamentals - DOLLAR AMOUNTS ROUNDED
    ia.purchase_price,
    ia.unit_count,
    ia.price_per_unit,
    ia.avg_rent_per_unit,
    ia.annual_rent_per_unit,
    ia.gross_annual_income,
    ia.annual_gross_income,
    ia.monthly_gross_income,
    ia.effective_gross_income,
    
    -- Core performance metrics - RATIOS KEEP PRECISION
    ia.opex_ratio,
    ia.vacancy_rate,
    ia.annual_operating_expenses,
    ia.annual_noi,
    ia.cap_rate,
    ia.cash_on_cash_return,
    ia.gross_yield_percent,
    ia.net_yield_after_fees,
    ia.gross_rent_multiplier,
    ia.price_to_income_ratio,
    
    -- Financial structure - DOLLAR AMOUNTS ROUNDED, RATIOS KEEP PRECISION
    ia.ds_ltv,
    ia.ds_int,
    ia.ds_term,
    ia.loan_amount,
    ia.monthly_debt_service,
    ia.annual_debt_service,
    ia.equity_investment,
    ia.debt_service_coverage_ratio,
    ia.debt_service_coverage_calc,
    
    -- Fee structure - DOLLAR AMOUNTS ROUNDED
    COALESCE(ia.acquisition_fee, 0) AS acquisition_fee,
    COALESCE(ia.annual_management_fee, 0) AS annual_management_fee,
    COALESCE(ia.estimated_disposition_fee, 0) AS estimated_disposition_fee,
    COALESCE(ia.total_annual_fees, 0) AS total_annual_fees,
    COALESCE(ia.management_fee_rate, 0) AS management_fee_rate,
    COALESCE(ia.management_fee_per_unit, 0) AS management_fee_per_unit,
    COALESCE(ia.fee_category, 'Standard') AS fee_category,
    
    -- Cash flow projections - DOLLAR AMOUNTS ROUNDED
    ia.annual_cash_flow_before_capex,
    ia.annual_capex_reserve,
    ia.annual_cash_flow_after_capex,
    ia.noi_after_fees,
    
    -- Performance ratios - KEEP RATIO PRECISION
    ia.noi_to_fees_ratio,
    ia.cash_flow_to_mgmt_fee_ratio,
    
    -- Unit economics - DOLLAR AMOUNTS ROUNDED
    ia.noi_per_unit,
    ia.cash_flow_per_unit,
    
    -- Business classifications for Metabase
    ia.risk_assessment,
    ia.investment_strategy,
    ia.investment_quality_rating,
    
    CASE 
        WHEN ia.unit_count <= 10 THEN 'Small Portfolio (1-10 units)'
        WHEN ia.unit_count <= 25 THEN 'Medium Portfolio (11-25 units)'
        WHEN ia.unit_count <= 50 THEN 'Large Portfolio (26-50 units)'
        ELSE 'Institutional (50+ units)'
    END AS property_size_category,
    
    CASE 
        WHEN ia.cash_on_cash_return >= 15 THEN 'Excellent Performance (15%+)'
        WHEN ia.cash_on_cash_return >= 10 THEN 'Strong Performance (10-15%)'
        WHEN ia.cash_on_cash_return >= 8 THEN 'Good Performance (8-10%)'
        ELSE 'Conservative Performance (<8%)'
    END AS performance_category,
    
    CASE 
        WHEN ia.debt_service_coverage_ratio >= 1.4 THEN 'Strong Coverage (1.4+)'
        WHEN ia.debt_service_coverage_ratio >= 1.2 THEN 'Adequate Coverage (1.2-1.4)'
        ELSE 'Tight Coverage (<1.2)'
    END AS coverage_category

FROM investment_analysis ia
WHERE ia.company_id = 1  -- Portfolio architecture filtering

ORDER BY ia.property_id