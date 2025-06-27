{{ config(materialized='view') }}

-- FIXED: Handle multiple assignment records per property by taking the latest operational status
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

-- FIXED: Use the latest assignment record per property to avoid duplicates
WHERE cf.property_id IN (
    SELECT DISTINCT ppa.property_id 
    FROM {{ source('hkh_dev', 'stg_property_portfolio_assignments') }} ppa
    JOIN {{ source('hkh_dev', 'stg_portfolio_settings') }} ps 
        ON ppa.portfolio_id = ps.portfolio_id 
        AND ppa.company_id = ps.company_id
    WHERE ps.company_id = 1 
      AND ps.is_default = TRUE
      AND ppa.assignment_id = (
          -- Get the latest assignment for this property
          SELECT MAX(ppa2.assignment_id)
          FROM {{ source('hkh_dev', 'stg_property_portfolio_assignments') }} ppa2
          WHERE ppa2.property_id = ppa.property_id
            AND ppa2.company_id = 1
      )
)

ORDER BY cf.property_id, cf.year