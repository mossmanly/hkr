-- HumanKindScore™ Calculator Model - INTEGRATED WITH RLV CALCULATOR
-- Now includes proper RLV scoring component

{{ config(
    materialized='table',
    schema='costar_analysis',
    indexes=[
        {'columns': ['analysis_run_id']},
        {'columns': ['humankind_score']},
        {'columns': ['property_id']}
    ]
) }}

WITH raw_properties AS (
    SELECT * FROM costar_analysis.raw_properties
    WHERE (not_ranked_reason IS NULL OR not_ranked_reason = '')
      AND number_of_units > 0
      AND list_price > 0
      AND price_per_unit > 0
),

-- Get RLV calculations from the intermediate model
rlv_data AS (
    SELECT 
        property_id,
        calculated_rlv,
        upside_percentage,
        upside_category,
        monthly_rent_per_unit,
        annual_expense_ratio
    FROM {{ ref('int_costar_rlv_calculator') }}
),

-- Use static weights
scoring_weights AS (
    SELECT 
        0.50 as rlv_weight_pct,
        0.20 as scale_weight_pct,
        0.15 as cost_weight_pct,
        0.15 as location_weight_pct
),

base_data AS (
    SELECT 
        rp.id as property_id,
        rp.upload_batch_id as analysis_run_id,
        rp.property_address,
        rp.city,
        rp.state,
        rp.number_of_units,
        rp.list_price,
        rp.price_per_unit,
        rp.year_built,
        rp.building_class,
        rp.created_at,
        
        -- RLV data from calculator
        COALESCE(rlv.calculated_rlv, 0) as calculated_rlv,
        COALESCE(rlv.upside_percentage, 0) as upside_percentage,
        COALESCE(rlv.upside_category, 'No RLV Data') as upside_category,
        COALESCE(rlv.monthly_rent_per_unit, 0) as monthly_rent_per_unit,
        COALESCE(rlv.annual_expense_ratio, 0.45) as annual_expense_ratio,
        
        -- Scoring weights
        sw.rlv_weight_pct,
        sw.scale_weight_pct,
        sw.cost_weight_pct,
        sw.location_weight_pct
        
    FROM raw_properties rp
    LEFT JOIN rlv_data rlv ON rp.id = rlv.property_id
    CROSS JOIN scoring_weights sw
),

scoring AS (
    SELECT *,
        
        -- 1. RLV Score (50% weight) - NOW ENABLED!
        CASE 
            WHEN upside_percentage >= 0.50 THEN (50 * rlv_weight_pct)       -- Exceptional upside
            WHEN upside_percentage >= 0.25 THEN (40 * rlv_weight_pct)       -- Excellent upside
            WHEN upside_percentage >= 0.10 THEN (30 * rlv_weight_pct)       -- Good upside
            WHEN upside_percentage >= 0.05 THEN (20 * rlv_weight_pct)       -- Fair upside
            WHEN upside_percentage >= 0.0 THEN (10 * rlv_weight_pct)        -- Break even
            WHEN upside_percentage >= -0.10 THEN (5 * rlv_weight_pct)       -- Slight overpay
            ELSE 0  -- Significantly overpriced
        END as rlv_score,
        
        CASE 
            WHEN upside_percentage >= 0.50 THEN 'Exceptional RLV Upside (50%+)'
            WHEN upside_percentage >= 0.25 THEN 'Excellent RLV Upside (25-50%)'
            WHEN upside_percentage >= 0.10 THEN 'Good RLV Upside (10-25%)'
            WHEN upside_percentage >= 0.05 THEN 'Fair RLV Upside (5-10%)'
            WHEN upside_percentage >= 0.0 THEN 'Break Even RLV (0-5%)'
            WHEN upside_percentage >= -0.10 THEN 'Slight Overpay (-10% to 0%)'
            ELSE 'Significantly Overpriced (<-10%)'
        END as rlv_reasoning,
        
        -- 2. Scale Score (20% weight)  
        CASE 
            WHEN number_of_units >= 50 THEN (50 * scale_weight_pct)       -- Large scale
            WHEN number_of_units >= 30 THEN (45 * scale_weight_pct)       -- Target scale (30+ premium)
            WHEN number_of_units >= 20 THEN (35 * scale_weight_pct)       -- Medium scale
            WHEN number_of_units >= 10 THEN (25 * scale_weight_pct)       -- Small scale
            WHEN number_of_units >= 5 THEN (15 * scale_weight_pct)        -- Very small
            ELSE (5 * scale_weight_pct)  -- Tiny scale
        END as scale_score,
        
        CASE 
            WHEN number_of_units >= 50 THEN 'Large Scale (50+ units)'
            WHEN number_of_units >= 30 THEN 'Target Scale (30+ units) - Premium'
            WHEN number_of_units >= 20 THEN 'Medium Scale (20-29 units)'
            WHEN number_of_units >= 10 THEN 'Small Scale (10-19 units)'
            WHEN number_of_units >= 5 THEN 'Very Small (5-9 units)'
            ELSE 'Tiny Scale (<5 units)'
        END as scale_reasoning,
        
        -- 3. Cost Efficiency Score (15% weight)
        CASE 
            WHEN price_per_unit <= 50000 THEN (50 * cost_weight_pct)      -- Exceptional value
            WHEN price_per_unit <= 75000 THEN (40 * cost_weight_pct)      -- Excellent value  
            WHEN price_per_unit <= 100000 THEN (35 * cost_weight_pct)     -- Very good value
            WHEN price_per_unit <= 125000 THEN (30 * cost_weight_pct)     -- Good value
            WHEN price_per_unit <= 150000 THEN (25 * cost_weight_pct)     -- Fair value
            WHEN price_per_unit <= 175000 THEN (20 * cost_weight_pct)     -- Below average value
            WHEN price_per_unit <= 200000 THEN (15 * cost_weight_pct)     -- Poor value
            ELSE (5 * cost_weight_pct)   -- Very expensive
        END as cost_efficiency_score,
        
        CASE 
            WHEN price_per_unit <= 50000 THEN 'Exceptional Value (<$50K/unit)'
            WHEN price_per_unit <= 75000 THEN 'Excellent Value ($50-75K/unit)'
            WHEN price_per_unit <= 100000 THEN 'Very Good Value ($75-100K/unit)'
            WHEN price_per_unit <= 125000 THEN 'Good Value ($100-125K/unit)'
            WHEN price_per_unit <= 150000 THEN 'Fair Value ($125-150K/unit)'
            WHEN price_per_unit <= 175000 THEN 'Below Average ($150-175K/unit)'
            WHEN price_per_unit <= 200000 THEN 'Poor Value ($175-200K/unit)'
            ELSE 'Very Expensive (>$200K/unit)'
        END as cost_efficiency_reasoning,
        
        -- 4. Location Score (15% weight)
        CASE 
            WHEN LOWER(city) IN ('portland', 'beaverton', 'hillsboro', 'gresham') THEN (50 * location_weight_pct)
            WHEN LOWER(state) = 'or' AND city IS NOT NULL THEN (35 * location_weight_pct)  -- Other Oregon cities
            WHEN LOWER(state) = 'wa' AND city IS NOT NULL THEN (25 * location_weight_pct)  -- Washington cities
            ELSE (10 * location_weight_pct)  -- Other locations
        END as location_score,
        
        CASE 
            WHEN LOWER(city) IN ('portland', 'beaverton', 'hillsboro', 'gresham') THEN 'Portland Metro Bonus'
            WHEN LOWER(state) = 'or' THEN 'Oregon Market'
            WHEN LOWER(state) = 'wa' THEN 'Washington Market'
            ELSE 'Other Market'
        END as location_reasoning,
        
        -- 5. Placeholder scores for future enrichment
        CAST(NULL AS NUMERIC) as school_quality_score,
        'Enrichment Data Not Available' as school_quality_reasoning,
        
        CAST(NULL AS NUMERIC) as healthcare_access_score,
        'Enrichment Data Not Available' as healthcare_reasoning,
        
        CAST(NULL AS NUMERIC) as transit_walkability_score,
        'Enrichment Data Not Available' as transit_reasoning
        
    FROM base_data
),

final_scoring AS (
    SELECT *,
        
        -- Calculate total HumanKindScore (NOW WITH RLV!)
        (rlv_score + scale_score + cost_efficiency_score + location_score + 
         COALESCE(school_quality_score, 0) + 
         COALESCE(healthcare_access_score, 0) + 
         COALESCE(transit_walkability_score, 0)) as humankind_score,
         
        -- Score tier classification (full scoring range)
        CASE 
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 45 THEN 'Excellent'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 35 THEN 'Good'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 25 THEN 'Fair'
            ELSE 'Poor'
        END as score_tier,
        
        -- Investment recommendation (now with RLV data)
        CASE 
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 40 THEN 'Strong Buy'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 35 THEN 'Consider'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 25 THEN 'Watch'
            ELSE 'Pass'
        END as investment_recommendation,
        
        -- Methodology version
        'v2.0_with_rlv' as methodology_version,
        
        -- Calculation timestamp
        CURRENT_TIMESTAMP as calculated_at,
        
        -- Updated metadata fields
        100 as total_possible_score,  -- Full scoring with RLV
        50 as current_max_score,      -- All current components max
        50 as enrichment_pending      -- Future enrichment data pending
        
    FROM scoring
),

final_output AS (
    SELECT *,
        CASE 
            WHEN investment_recommendation = 'Strong Buy' THEN 
                'Exceptional opportunity (Score: ' || ROUND(humankind_score, 1) || ') - Strong RLV upside with good fundamentals'
            WHEN investment_recommendation = 'Consider' THEN 
                'Good opportunity (Score: ' || ROUND(humankind_score, 1) || ') - Solid RLV metrics and fundamentals'
            WHEN investment_recommendation = 'Watch' THEN 
                'Monitor opportunity (Score: ' || ROUND(humankind_score, 1) || ') - Some positives, watch for improvements'
            ELSE 'Below target criteria (Score: ' || ROUND(humankind_score, 1) || ') - Does not meet investment thresholds'
        END as recommendation_reasons,
        
        CURRENT_TIMESTAMP as score_created_at,
        CURRENT_TIMESTAMP as score_updated_at
        
    FROM final_scoring
)

SELECT 
    property_id,
    analysis_run_id,
    -- Basic property info (for joins and reference)
    property_address,
    city,
    state,
    number_of_units,
    list_price,
    price_per_unit,
    year_built,
    building_class,
    -- RLV components (now included!)
    calculated_rlv,
    upside_percentage,
    upside_category,
    rlv_score,
    rlv_reasoning,
    -- Other scoring components
    location_score,
    location_reasoning,
    scale_score,
    scale_reasoning,
    cost_efficiency_score,
    cost_efficiency_reasoning,
    school_quality_score,
    school_quality_reasoning,
    healthcare_access_score,
    healthcare_reasoning,
    transit_walkability_score,
    transit_reasoning,
    -- Final scores
    humankind_score,
    score_tier,
    investment_recommendation,
    recommendation_reasons,
    methodology_version,
    calculated_at,
    total_possible_score,
    current_max_score,
    enrichment_pending,
    score_created_at,
    score_updated_at
FROM final_output
WHERE property_id IS NOT NULL
ORDER BY humankind_score DESC 