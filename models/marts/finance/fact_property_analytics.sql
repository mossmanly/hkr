-- marts/finance/fact_property_analytics.sql
-- Property investment analytics and KPIs for Metabase dashboards
-- UPDATED: Uses new fee mart instead of old int_fee_calculations

{{ config(materialized='table') }}

WITH property_fees AS (
    -- Aggregate new detailed fee mart to property level
    SELECT 
        fc.property_id,
        fc.property_name,
        fc.unit_count,
        fc.purchase_price,
        
        -- Calculate fee totals from detailed categories
        0 AS acquisition_fee,  -- Not in operational fee model
        ROUND(SUM(CASE WHEN fc.category IN ('asset_mgmt', 'property_mgmt') THEN fc.fee_amount ELSE 0 END), 0) AS annual_management_fee,
        0 AS estimated_disposition_fee,  -- Not in operational fee model
        ROUND(SUM(fc.fee_amount), 0) AS total_annual_fees,
        
        -- Calculate average fee rates
        ROUND(AVG(fc.fee_pct_of_pgi), 2) AS management_fee_rate,
        ROUND(SUM(fc.fee_amount) / fc.unit_count, 0) AS management_fee_per_unit,
        'Standard' AS fee_category  -- Default since not in new model

    FROM {{ ref('fact_opex_fees_calculations') }} fc
    WHERE fc.year = 1  -- First year data
    GROUP BY fc.property_id, fc.property_name, fc.unit_count, fc.purchase_price
),

property_data AS (
    SELECT 
        -- Property identification from property inputs
        spi.property_id,
        spi.company_id,
        spi.portfolio_id,
        pf.property_name,
        ROUND(pf.purchase_price, 0) AS purchase_price,
        pf.unit_count,
        ROUND(spi.avg_rent_per_unit * spi.unit_count * 12, 0) AS gross_annual_income,
        ps.portfolio_name,
        ps.investment_strategy,
        
        -- Fee data from aggregated fees
        COALESCE(pf.acquisition_fee, 0) AS acquisition_fee,
        COALESCE(pf.annual_management_fee, 0) AS annual_management_fee,
        COALESCE(pf.estimated_disposition_fee, 0) AS estimated_disposition_fee,
        COALESCE(pf.total_annual_fees, 0) AS total_annual_fees,
        COALESCE(pf.management_fee_rate, 0) AS management_fee_rate,
        COALESCE(pf.management_fee_per_unit, 0) AS management_fee_per_unit,
        COALESCE(pf.fee_category, 'Standard') AS fee_category,
        
        -- Cash flow data from int_property_cash_flows (year 1 data)
        ROUND(pcf.annual_noi, 0) AS annual_noi,
        ROUND(pcf.egi, 0) AS egi,
        ROUND(pcf.opex, 0) AS opex,
        ROUND(pcf.noi, 0) AS noi,
        ROUND(pcf.debt_service, 0) AS debt_service,
        ROUND(pcf.capex, 0) AS capex,
        ROUND(pcf.btcf, 0) AS btcf,
        ROUND(pcf.btcf_after_capex, 0) AS btcf_after_capex,
        ROUND(pcf.annual_cash_flow_after_capex, 0) AS annual_cash_flow_after_capex,
        ROUND(pcf.capex_float_income, 0) AS capex_float_income,
        ROUND(pcf.reserve_balance, 0) AS reserve_balance,
        
        -- Additional property data from stg_property_inputs
        spi.property_address,
        spi.city,
        spi.zip,
        ROUND(spi.avg_rent_per_unit, 0) AS avg_rent_per_unit,
        ROUND(spi.price_per_unit, 0) AS price_per_unit,
        spi.building_class,
        spi.acquisition_year

    FROM hkh_dev.stg_property_inputs spi
    LEFT JOIN property_fees pf ON spi.property_id = pf.property_id
    LEFT JOIN {{ ref('int_property_cash_flows') }} pcf
        ON spi.property_id = pcf.property_id
        AND spi.company_id = pcf.company_id
        AND spi.portfolio_id = pcf.portfolio_id
        AND pcf.year = 1  -- First year projections
    LEFT JOIN hkh_dev.stg_portfolio_settings ps
        ON spi.company_id = ps.company_id
        AND spi.portfolio_id = ps.portfolio_id
    WHERE spi.company_id = 1
)

SELECT 
    -- Property identification
    pd.property_id,
    pd.company_id,
    pd.portfolio_id,
    pd.property_name,
    pd.property_address,
    pd.city,
    pd.zip,
    pd.building_class,
    pd.acquisition_year,
    pd.portfolio_name,
    pd.investment_strategy,
    
    -- Property fundamentals
    pd.purchase_price,
    pd.unit_count,
    pd.price_per_unit,
    pd.avg_rent_per_unit,
    ROUND(pd.avg_rent_per_unit * 12, 0) AS annual_rent_per_unit,
    pd.gross_annual_income,
    
    -- Core performance metrics
    pd.annual_noi,
    ROUND(pd.annual_noi / NULLIF(pd.purchase_price, 0) * 100, 2) AS cap_rate,
    ROUND(pd.annual_cash_flow_after_capex / NULLIF(pd.purchase_price - (pd.purchase_price * 0.75), 0) * 100, 2) AS estimated_cash_on_cash_return,
    ROUND((pd.avg_rent_per_unit * 12) / NULLIF(pd.price_per_unit, 0) * 100, 2) AS gross_yield_percent,
    ROUND(pd.gross_annual_income / pd.purchase_price * 100, 2) AS gross_rent_multiplier,
    ROUND(pd.purchase_price / pd.gross_annual_income, 1) AS price_to_income_ratio,
    
    -- Cash flow performance
    pd.egi,
    pd.opex,
    pd.noi,
    pd.debt_service,
    pd.btcf,
    pd.capex,
    pd.btcf_after_capex,
    pd.annual_cash_flow_after_capex,
    pd.capex_float_income,
    pd.reserve_balance,
    ROUND(pd.annual_noi / NULLIF(pd.debt_service, 0), 2) AS debt_service_coverage_ratio,
    
    -- Fee structure
    pd.acquisition_fee,
    pd.annual_management_fee,
    pd.estimated_disposition_fee,
    pd.total_annual_fees,
    pd.management_fee_rate,
    pd.management_fee_per_unit,
    pd.fee_category,
    
    -- Performance ratios and metrics
    ROUND(pd.annual_noi - pd.total_annual_fees, 0) AS noi_after_fees,
    ROUND((pd.annual_noi - pd.total_annual_fees) / pd.purchase_price * 100, 2) AS net_yield_after_fees,
    ROUND(pd.annual_noi / NULLIF(pd.total_annual_fees, 0), 2) AS noi_to_fees_ratio,
    ROUND(pd.annual_cash_flow_after_capex / NULLIF(pd.annual_management_fee, 0), 2) AS cash_flow_to_mgmt_fee_ratio,
    
    -- Unit economics
    ROUND(pd.annual_noi / pd.unit_count, 0) AS noi_per_unit,
    ROUND(pd.annual_cash_flow_after_capex / pd.unit_count, 0) AS cash_flow_per_unit,
    
    -- Business classifications
    CASE 
        WHEN pd.annual_cash_flow_after_capex / NULLIF(pd.purchase_price * 0.25, 0) >= 0.12 THEN 'Low Risk (High Return)'
        WHEN pd.annual_cash_flow_after_capex / NULLIF(pd.purchase_price * 0.25, 0) >= 0.08 THEN 'Medium Risk'
        WHEN pd.annual_cash_flow_after_capex / NULLIF(pd.purchase_price * 0.25, 0) >= 0.05 THEN 'Higher Risk (Low Return)'
        ELSE 'High Risk'
    END AS risk_assessment,
    
    CASE 
        WHEN pd.annual_noi / NULLIF(pd.purchase_price, 0) >= 0.08 THEN 'Value Play (High Cap)'
        WHEN pd.annual_noi / NULLIF(pd.purchase_price, 0) >= 0.06 THEN 'Balanced Investment'
        ELSE 'Growth Play (Low Cap)'
    END AS investment_strategy_calc,
    
    CASE 
        WHEN pd.unit_count <= 10 THEN 'Small Portfolio (1-10 units)'
        WHEN pd.unit_count <= 25 THEN 'Medium Portfolio (11-25 units)'
        WHEN pd.unit_count <= 50 THEN 'Large Portfolio (26-50 units)'
        ELSE 'Institutional (50+ units)'
    END AS property_size_category,
    
    CASE 
        WHEN ROUND(pd.annual_cash_flow_after_capex / NULLIF(pd.purchase_price * 0.25, 0) * 100, 2) >= 15 THEN 'Excellent Performance (15%+)'
        WHEN ROUND(pd.annual_cash_flow_after_capex / NULLIF(pd.purchase_price * 0.25, 0) * 100, 2) >= 10 THEN 'Strong Performance (10-15%)'
        WHEN ROUND(pd.annual_cash_flow_after_capex / NULLIF(pd.purchase_price * 0.25, 0) * 100, 2) >= 8 THEN 'Good Performance (8-10%)'
        ELSE 'Conservative Performance (<8%)'
    END AS performance_category,
    
    CASE 
        WHEN ROUND(pd.annual_noi / NULLIF(pd.debt_service, 0), 2) >= 1.4 THEN 'Strong Coverage (1.4+)'
        WHEN ROUND(pd.annual_noi / NULLIF(pd.debt_service, 0), 2) >= 1.2 THEN 'Adequate Coverage (1.2-1.4)'
        ELSE 'Tight Coverage (<1.2)'
    END AS coverage_category

FROM property_data pd
WHERE pd.company_id = 1

ORDER BY pd.property_id 