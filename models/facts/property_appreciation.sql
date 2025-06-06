-- property_appreciation.sql (dbt model)
-- Multi-scenario property appreciation calculator
-- Generates conservative/baseline/aggressive appreciation for each property by year

{{ config(materialized='view') }}

WITH year_series AS (
    -- Generate years 1 through 30 for each property
    SELECT 
        property_id,
        year_num
    FROM {{ source('inputs', 'property_inputs') }}
    CROSS JOIN (
        SELECT ROW_NUMBER() OVER () AS year_num
        FROM (VALUES (1),(2),(3),(4),(5),(6),(7),(8),(9),(10),
                     (11),(12),(13),(14),(15),(16),(17),(18),(19),(20),
                     (21),(22),(23),(24),(25),(26),(27),(28),(29),(30)
        ) AS years(year_num)
    ) years
),

appreciation_calculations AS (
    SELECT 
        ys.property_id,
        ys.year_num,
        pi.purchase_price,
        
        -- Conservative Scenario
        'conservative' AS scenario,
        pi.apprec_conservative_rate AS appreciation_rate,
        pi.purchase_price * POWER(1 + pi.apprec_conservative_rate, ys.year_num - 1) AS current_value,
        (pi.purchase_price * POWER(1 + pi.apprec_conservative_rate, ys.year_num - 1)) - pi.purchase_price AS total_appreciation,
        CASE 
            WHEN ys.year_num = 1 THEN 0
            ELSE (pi.purchase_price * POWER(1 + pi.apprec_conservative_rate, ys.year_num - 1)) - 
                 (pi.purchase_price * POWER(1 + pi.apprec_conservative_rate, ys.year_num - 2))
        END AS annual_appreciation
        
    FROM year_series ys
    JOIN {{ source('inputs', 'property_inputs') }} pi
        ON ys.property_id = pi.property_id
    
    UNION ALL
    
    SELECT 
        ys.property_id,
        ys.year_num,
        pi.purchase_price,
        
        -- Baseline Scenario
        'baseline' AS scenario,
        pi.apprec_baseline_rate AS appreciation_rate,
        pi.purchase_price * POWER(1 + pi.apprec_baseline_rate, ys.year_num - 1) AS current_value,
        (pi.purchase_price * POWER(1 + pi.apprec_baseline_rate, ys.year_num - 1)) - pi.purchase_price AS total_appreciation,
        CASE 
            WHEN ys.year_num = 1 THEN 0
            ELSE (pi.purchase_price * POWER(1 + pi.apprec_baseline_rate, ys.year_num - 1)) - 
                 (pi.purchase_price * POWER(1 + pi.apprec_baseline_rate, ys.year_num - 2))
        END AS annual_appreciation
        
    FROM year_series ys
    JOIN {{ source('inputs', 'property_inputs') }} pi
        ON ys.property_id = pi.property_id
    
    UNION ALL
    
    SELECT 
        ys.property_id,
        ys.year_num,
        pi.purchase_price,
        
        -- Aggressive Scenario
        'aggressive' AS scenario,
        pi.apprec_aggressive_rate AS appreciation_rate,
        pi.purchase_price * POWER(1 + pi.apprec_aggressive_rate, ys.year_num - 1) AS current_value,
        (pi.purchase_price * POWER(1 + pi.apprec_aggressive_rate, ys.year_num - 1)) - pi.purchase_price AS total_appreciation,
        CASE 
            WHEN ys.year_num = 1 THEN 0
            ELSE (pi.purchase_price * POWER(1 + pi.apprec_aggressive_rate, ys.year_num - 1)) - 
                 (pi.purchase_price * POWER(1 + pi.apprec_aggressive_rate, ys.year_num - 2))
        END AS annual_appreciation
        
    FROM year_series ys
    JOIN {{ source('inputs', 'property_inputs') }} pi
        ON ys.property_id = pi.property_id
)

SELECT 
    property_id,
    year_num AS year,
    scenario,
    appreciation_rate,
    purchase_price,
    ROUND(current_value, 0) AS current_value,
    ROUND(total_appreciation, 0) AS total_appreciation,
    ROUND(annual_appreciation, 0) AS annual_appreciation,
    
    -- Additional calculated metrics
    ROUND((current_value / purchase_price - 1), 4) AS cumulative_appreciation_multiple,
    ROUND((total_appreciation / purchase_price), 4) AS appreciation_roi

FROM appreciation_calculations

ORDER BY property_id, scenario, year