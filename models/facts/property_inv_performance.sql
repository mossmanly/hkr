-- property_inv_performance.sql (dbt model)
-- Time Series Performance for trending, filtering, and multi-year analysis
-- Updates automatically when underlying data changes

{{ config(materialized='view') }}

SELECT 
    cf.property_id,
    cf.year,
    
    -- Cash Flow Metrics
    cf.atcf,
    cf.noi,
    (cf.atcf / pi.purchase_price) AS annual_coc,
    
    -- Cumulative Cash Flow
    SUM(cf.atcf) OVER (
        PARTITION BY cf.property_id 
        ORDER BY cf.year 
        ROWS UNBOUNDED PRECEDING
    ) AS cumulative_atcf,
    
    -- Cumulative CoC
    SUM(cf.atcf) OVER (
        PARTITION BY cf.property_id 
        ORDER BY cf.year 
        ROWS UNBOUNDED PRECEDING
    ) / pi.purchase_price AS cumulative_coc,
    
    -- Property Details
    pi.purchase_price

FROM {{ source('hkh_dev', 'fact_property_cash_flow') }} cf
JOIN {{ source('inputs', 'property_inputs') }} pi
    ON cf.property_id = pi.property_id

ORDER BY cf.property_id, cf.year