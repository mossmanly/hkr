-- property_investor_performance.sql (dbt model)
-- Time Series Performance for trending, filtering, and multi-year analysis
-- Updates automatically when underlying data changes
-- UPDATED: Portfolio filtering with company scoping

{{ config(materialized='view') }}

SELECT 
    cf.property_id,
    cf.year,
    
    -- Cash Flow Metrics
    cf.atcf_operations,
    cf.noi,
    (cf.atcf_operations / pi.purchase_price) AS annual_coc,
    
    -- Cumulative Cash Flow
    SUM(cf.atcf_operations) OVER (
        PARTITION BY cf.property_id 
        ORDER BY cf.year 
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_atcf,
    
    -- Cumulative CoC
    SUM(cf.atcf_operations) OVER (
        PARTITION BY cf.property_id 
        ORDER BY cf.year 
        ROWS UNBOUNDED PRECEDING
    ) / pi.purchase_price AS cumulative_coc,
    
    -- Property Details
    pi.purchase_price

FROM {{ ref('int_property_cash_flows') }} cf
JOIN {{ source('hkh_dev', 'stg_property_inputs') }} pi
    ON cf.property_id = pi.property_id

-- Portfolio filtering: Only include properties in default portfolio for this company
INNER JOIN {{ source('hkh_dev', 'stg_property_portfolio_assignments') }} ppa 
    ON pi.property_id = ppa.property_id
INNER JOIN {{ source('hkh_dev', 'stg_portfolio_settings') }} ps 
    ON ppa.portfolio_id = ps.portfolio_id 
    AND ppa.company_id = ps.company_id

WHERE ps.company_id = 1  -- Company scoping for future multi-tenancy
  AND ps.is_default = TRUE  -- Only include default portfolio properties

ORDER BY cf.property_id, cf.year 