-- vw_clean_humankind_analysis.sql
-- This is the view your Metabase dashboard is actually looking for!

{{ config(
    materialized='view',
    schema='costar_analysis'
) }}

WITH latest_run AS (
    SELECT MAX(analysis_run_id) as latest_run_id 
    FROM {{ ref('fct_humankind_scores') }}
),

ranked_properties AS (
    SELECT 
        h.*,
        ROW_NUMBER() OVER (ORDER BY h.humankind_score DESC) as rank_overall
    FROM {{ ref('fct_humankind_scores') }} h
    INNER JOIN latest_run lr ON h.analysis_run_id = lr.latest_run_id
)

SELECT 
    property_id,
    property_address,
    city,
    state,
    number_of_units,
    ROUND(list_price, 0) as list_price,                    -- No decimals on dollars
    ROUND(price_per_unit, 0) as price_per_unit,            -- No decimals on dollars
    ROUND(calculated_rlv, 0) as calculated_rlv,            -- No decimals on dollars
    ROUND(humankind_score, 2) as humankind_score,          -- 2 decimals on scores
    ROUND(rlv_score, 2) as rlv_score,                      -- 2 decimals on scores
    ROUND(scale_score, 2) as scale_score,                  -- 2 decimals on scores
    ROUND(cost_efficiency_score, 2) as cost_efficiency_score, -- 2 decimals on scores
    ROUND(location_score, 2) as location_score,            -- 2 decimals on scores
    score_tier,
    investment_recommendation,
    upside_category,
    rank_overall,
    analysis_run_id as run_name,
    calculated_at
FROM ranked_properties
ORDER BY humankind_score DESC