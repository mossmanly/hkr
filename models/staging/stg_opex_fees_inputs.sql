{{ config(materialized='table') }}

-- OPEX fees configuration table - single source of truth
-- Contains base fee percentages and inflation rates for professional fees

SELECT 
    company_id,
    portfolio_id, 
    fee_component,
    annual_inflation_rate,
    last_market_review_date,
    market_benchmark_source,
    next_review_due_date,
    variance_from_market,
    review_frequency_months,
    notes,
    created_by,
    created_at,
    updated_at,
    base_pct_of_pgi
FROM (
    SELECT 1 as company_id, 'micro-1' as portfolio_id, 'property_mgmt' as fee_component, 0.04 as annual_inflation_rate, 
           NULL as last_market_review_date, NULL as market_benchmark_source, NULL as next_review_due_date, 
           NULL as variance_from_market, 12 as review_frequency_months, 'Property management fee' as notes, 
           NULL as created_by, NULL as created_at, NULL as updated_at, 8.5 as base_pct_of_pgi
    UNION ALL
    SELECT 1, 'micro-1', 'asset_mgmt', 0.025, 
           NULL, NULL, NULL, NULL, 12, 'Asset management fee', 
           NULL, NULL, NULL, 2.0
    UNION ALL
    SELECT 1, 'micro-1', 'leasing', 0.015, 
           NULL, NULL, NULL, NULL, 12, 'Leasing fee', 
           NULL, NULL, NULL, 0.8
    UNION ALL
    SELECT 1, 'micro-1', 'maintenance', 0.05, 
           NULL, NULL, NULL, NULL, 12, 'Maintenance coordination fee', 
           NULL, NULL, NULL, 1.2
) t