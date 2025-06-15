-- HumanKindScore™ Calculator Model - ROBUST EXTENSIBLE VERSION
-- Maintains sophisticated structure while fixing breaking points

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
),

-- Use static weights since intermediate models don't exist yet
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
        
        -- Placeholder RLV data (since int_costar_rlv_calculator doesn't exist yet)
        0 as calculated_rlv,
        0 as upside_percentage,
        'RLV Calculator Not Available' as upside_category,
        CASE 
            WHEN rp.number_of_units > 0 AND rp.list_price > 0 
            THEN ROUND((rp.list_price / rp.number_of_units * 0.008), 0)
            ELSE 0 
        END as monthly_rent_per_unit,
        0.30 as annual_expense_ratio,
        
        -- Scoring weights
        sw.rlv_weight_pct,
        sw.scale_weight_pct,
        sw.cost_weight_pct,
        sw.location_weight_pct
        
    FROM raw_properties rp
    CROSS JOIN scoring_weights sw
),

scoring AS (
    SELECT *,
        
        -- 1. RLV Score (50% weight) - Currently disabled since RLV calc doesn't exist
        0 as rlv_score,
        'RLV Calculator Not Available - Scoring Disabled' as rlv_reasoning,
        
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
        
        -- Calculate total HumanKindScore (without RLV component for now)
        (rlv_score + scale_score + cost_efficiency_score + location_score + 
         COALESCE(school_quality_score, 0) + 
         COALESCE(healthcare_access_score, 0) + 
         COALESCE(transit_walkability_score, 0)) as humankind_score,
         
        -- Score tier classification (adjusted for reduced scoring without RLV)
        CASE 
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 20 THEN 'Excellent'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 15 THEN 'Good'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 10 THEN 'Fair'
            ELSE 'Poor'
        END as score_tier,
        
        -- Investment recommendation (conservative without RLV data)
        CASE 
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 20 THEN 'Consider'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 15 THEN 'Watch'
            ELSE 'Pass'
        END as investment_recommendation,
        
        -- Methodology version
        'v1.2_no_rlv_calc' as methodology_version,
        
        -- Calculation timestamp
        CURRENT_TIMESTAMP as calculated_at,
        
        -- Metadata fields
        50 as total_possible_score,  -- Reduced without RLV component
        35 as current_max_score,     -- Scale + Cost + Location max
        25 as enrichment_pending     -- RLV + enrichment data pending
        
    FROM scoring
),

final_output AS (
    SELECT *,
        CASE 
            WHEN investment_recommendation = 'Consider' THEN 
                'Good fundamentals (Score: ' || ROUND(humankind_score, 1) || ') - RLV analysis pending'
            WHEN investment_recommendation = 'Watch' THEN 
                'Fair fundamentals (Score: ' || ROUND(humankind_score, 1) || ') - Monitor for improvements'
            ELSE 'Below target criteria (Score: ' || ROUND(humankind_score, 1) || ')'
        END as recommendation_reasons,
        
        CURRENT_TIMESTAMP as score_created_at,
        CURRENT_TIMESTAMP as score_updated_at
        
    FROM final_scoring
)

SELECT 
    property_id,
    analysis_run_id,
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