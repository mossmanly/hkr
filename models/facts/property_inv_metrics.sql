-- property_inv_metrics.sql (dbt model) 
-- Single Summary Metrics for portfolio comparison and executive reporting
-- Updates automatically when underlying data changes
-- UPDATED: Portfolio filtering with company scoping

{{ config(materialized='view') }}

SELECT 
    pi.property_id,
    
    -- Acquisition Data
    pi.purchase_price,
    (cf_year1.noi / pi.purchase_price) AS initial_cap_rate,
    
    -- Lifecycle Performance Aggregates
    AVG(perf.annual_coc) AS average_annual_coc,
    
    -- Hold Period Returns (specific year snapshots)
    MAX(CASE WHEN perf.year = 5 THEN perf.cumulative_coc END) AS year_5_cumulative_coc,
    MAX(CASE WHEN perf.year = 10 THEN perf.cumulative_coc END) AS year_10_cumulative_coc,
    MAX(CASE WHEN perf.year = 15 THEN perf.cumulative_coc END) AS year_15_cumulative_coc,
    MAX(CASE WHEN perf.year = 20 THEN perf.cumulative_coc END) AS year_20_cumulative_coc,
    
    -- Multi-Scenario Cap Rates (NOI / Current Property Value)
    -- Year 5 Cap Rates
    MAX(CASE WHEN cf_noi.year = 5 AND appr.scenario = 'conservative' AND appr.year = 5 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_5_cap_rate_conservative,
    MAX(CASE WHEN cf_noi.year = 5 AND appr.scenario = 'baseline' AND appr.year = 5 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_5_cap_rate_baseline,
    MAX(CASE WHEN cf_noi.year = 5 AND appr.scenario = 'aggressive' AND appr.year = 5 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_5_cap_rate_aggressive,
    
    -- Year 10 Cap Rates
    MAX(CASE WHEN cf_noi.year = 10 AND appr.scenario = 'conservative' AND appr.year = 10 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_10_cap_rate_conservative,
    MAX(CASE WHEN cf_noi.year = 10 AND appr.scenario = 'baseline' AND appr.year = 10 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_10_cap_rate_baseline,
    MAX(CASE WHEN cf_noi.year = 10 AND appr.scenario = 'aggressive' AND appr.year = 10 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_10_cap_rate_aggressive,
    
    -- Year 15 Cap Rates
    MAX(CASE WHEN cf_noi.year = 15 AND appr.scenario = 'conservative' AND appr.year = 15 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_15_cap_rate_conservative,
    MAX(CASE WHEN cf_noi.year = 15 AND appr.scenario = 'baseline' AND appr.year = 15 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_15_cap_rate_baseline,
    MAX(CASE WHEN cf_noi.year = 15 AND appr.scenario = 'aggressive' AND appr.year = 15 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_15_cap_rate_aggressive,
    
    -- Year 20 Cap Rates
    MAX(CASE WHEN cf_noi.year = 20 AND appr.scenario = 'conservative' AND appr.year = 20 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_20_cap_rate_conservative,
    MAX(CASE WHEN cf_noi.year = 20 AND appr.scenario = 'baseline' AND appr.year = 20 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_20_cap_rate_baseline,
    MAX(CASE WHEN cf_noi.year = 20 AND appr.scenario = 'aggressive' AND appr.year = 20 
         THEN ROUND((cf_noi.noi / NULLIF(appr.current_value, 0)), 4) END) AS year_20_cap_rate_aggressive,
    
    -- Total Return Metrics
    MAX(perf.cumulative_atcf) AS total_cash_returned,
    MAX(perf.cumulative_coc) AS total_coc_multiple
    
FROM {{ source('inputs', 'property_inputs') }} pi

-- Portfolio filtering: Only include properties in default portfolio for this company
INNER JOIN {{ source('inputs', 'property_portfolio_assignments') }} ppa 
    ON pi.property_id = ppa.property_id
INNER JOIN {{ source('inputs', 'portfolio_settings') }} ps 
    ON ppa.portfolio_id = ps.portfolio_id 
    AND ppa.company_id = ps.company_id
    
-- Join to get year 1 NOI for initial cap rate
LEFT JOIN {{ ref('fact_property_cash_flow') }} cf_year1 
    ON pi.property_id = cf_year1.property_id 
    AND cf_year1.year = 1

-- Join to cash flow data for NOI in target years
LEFT JOIN {{ ref('fact_property_cash_flow') }} cf_noi
    ON pi.property_id = cf_noi.property_id
    AND cf_noi.year IN (5, 10, 15, 20)

-- Join to appreciation data for current values by scenario
LEFT JOIN {{ ref('property_appreciation') }} appr
    ON pi.property_id = appr.property_id
    AND appr.year IN (5, 10, 15, 20)

-- Join to performance data for aggregations
LEFT JOIN {{ ref('property_inv_performance') }} perf
    ON pi.property_id = perf.property_id

WHERE ps.company_id = 1  -- Company scoping for future multi-tenancy
  AND ps.is_default = TRUE  -- Only include default portfolio properties

GROUP BY 
    pi.property_id,
    pi.purchase_price, 
    cf_year1.noi