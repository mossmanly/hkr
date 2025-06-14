-- HumanKindScore™ Calculator Model - ORIGINAL WORKING VERSION
-- Back to what was working before we added complexity

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['analysis_run_id']},
        {'columns': ['humankind_score']},
        {'columns': ['property_id']}
    ]
) }}

WITH raw_properties AS (
    SELECT * FROM {{ source('costar_analysis', 'raw_properties') }}
),

rlv_calculations AS (
    SELECT * FROM {{ ref('int_costar_rlv_calculator') }}
),

scoring_weights AS (
    SELECT * FROM {{ ref('int_rlv_assumptions') }}
),

-- Join raw properties with RLV calculations and scoring weights
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
        
        -- RLV data from your calculator
        rlv.calculated_rlv,
        rlv.upside_percentage,
        rlv.upside_category,
        rlv.monthly_rent_per_unit,
        rlv.annual_expense_ratio,
        
        -- Scoring weights from your assumptions
        sw.rlv_weight_pct,
        sw.scale_weight_pct,
        sw.cost_weight_pct,
        sw.location_weight_pct
        
    FROM raw_properties rp
    LEFT JOIN rlv_calculations rlv 
        ON rp.id = rlv.property_id
    CROSS JOIN scoring_weights sw
),

-- Calculate HumanKindScore components
scoring AS (
    SELECT *,
        
        -- 1. RLV Score (50% weight from your assumptions)
        CASE 
            WHEN upside_percentage >= 1.0 THEN (50 * rlv_weight_pct)      -- 100%+ upside = full points
            WHEN upside_percentage >= 0.50 THEN (40 * rlv_weight_pct)     -- 50%+ upside
            WHEN upside_percentage >= 0.25 THEN (30 * rlv_weight_pct)     -- 25%+ upside  
            WHEN upside_percentage >= 0.10 THEN (20 * rlv_weight_pct)     -- 10%+ upside
            WHEN upside_percentage >= 0.0 THEN (10 * rlv_weight_pct)      -- Break even
            ELSE 0  -- No RLV data
        END as rlv_score,
        
        COALESCE(upside_category, 'No RLV Data Available') as rlv_reasoning,
        
        -- 2. Scale Score (20% weight from your assumptions)  
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
        
        -- 3. Cost Efficiency Score (15% weight from your assumptions)
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
        
        -- 4. Location Score (15% weight from your assumptions)
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
        NULL as school_quality_score,
        'Enrichment Data Not Available' as school_quality_reasoning,
        
        NULL as healthcare_access_score,
        'Enrichment Data Not Available' as healthcare_reasoning,
        
        NULL as transit_walkability_score,
        'Enrichment Data Not Available' as transit_reasoning
        
    FROM base_data
),

-- Calculate final scores and recommendations
final_scoring AS (
    SELECT *,
        
        -- Calculate total HumanKindScore using your weighted methodology
        (rlv_score + scale_score + cost_efficiency_score + location_score + 
         COALESCE(school_quality_score::numeric, 0) + 
         COALESCE(healthcare_access_score::numeric, 0) + 
         COALESCE(transit_walkability_score::numeric, 0)) as humankind_score,
         
        -- Score tier classification (adjusted for your methodology)
        CASE 
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score::numeric, 0) + 
                  COALESCE(healthcare_access_score::numeric, 0) + 
                  COALESCE(transit_walkability_score::numeric, 0)) >= 40 THEN 'Exceptional'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score::numeric, 0) + 
                  COALESCE(healthcare_access_score::numeric, 0) + 
                  COALESCE(transit_walkability_score::numeric, 0)) >= 30 THEN 'Excellent'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score::numeric, 0) + 
                  COALESCE(healthcare_access_score::numeric, 0) + 
                  COALESCE(transit_walkability_score::numeric, 0)) >= 20 THEN 'Good'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score::numeric, 0) + 
                  COALESCE(healthcare_access_score::numeric, 0) + 
                  COALESCE(transit_walkability_score::numeric, 0)) >= 10 THEN 'Fair'
            ELSE 'Poor'
        END as score_tier,
        
        -- Investment recommendation
        CASE 
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score::numeric, 0) + 
                  COALESCE(healthcare_access_score::numeric, 0) + 
                  COALESCE(transit_walkability_score::numeric, 0)) >= 40 
                  AND upside_percentage >= 0.25 THEN 'Strong Buy'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score::numeric, 0) + 
                  COALESCE(healthcare_access_score::numeric, 0) + 
                  COALESCE(transit_walkability_score::numeric, 0)) >= 30 
                  AND upside_percentage >= 0.10 THEN 'Buy'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score::numeric, 0) + 
                  COALESCE(healthcare_access_score::numeric, 0) + 
                  COALESCE(transit_walkability_score::numeric, 0)) >= 20 THEN 'Consider'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + 
                  COALESCE(school_quality_score::numeric, 0) + 
                  COALESCE(healthcare_access_score::numeric, 0) + 
                  COALESCE(transit_walkability_score::numeric, 0)) >= 10 THEN 'Watch'
            ELSE 'Pass'
        END as investment_recommendation,
        
        -- Methodology version for tracking
        'v1.0_basic_costar' as methodology_version,
        
        -- Calculation timestamp
        CURRENT_TIMESTAMP as calculated_at,
        
        -- Detailed calculation breakdown
        JSON_BUILD_OBJECT(
            'total_possible_score', 100,
            'current_max_score', 70,
            'enrichment_pending', 30,
            'location_weight', '15%',
            'units_weight', '20%', 
            'cost_efficiency_weight', '15%',
            'rlv_weight', '50%',
            'school_weight', '0%',
            'healthcare_weight', '0%',
            'transit_weight', '0%',
            'missing_components', ARRAY['school_quality', 'healthcare_access', 'transit_walkability']
        ) as calculation_details
        
    FROM scoring
),

-- Add recommendation reasons
final_output AS (
    SELECT *,
        CASE 
            WHEN investment_recommendation = 'Strong Buy' THEN 
                'High HumanKindScore (' || humankind_score || ') + Strong RLV upside (' || 
                ROUND((upside_percentage * 100)::numeric, 1) || '%)'
            WHEN investment_recommendation = 'Buy' THEN 
                'Good HumanKindScore (' || humankind_score || ') + Positive RLV upside (' || 
                ROUND((upside_percentage * 100)::numeric, 1) || '%)'
            WHEN investment_recommendation = 'Consider' THEN 
                'Decent fundamentals, review RLV analysis and market conditions'
            WHEN investment_recommendation = 'Watch' THEN 
                'Below target criteria, monitor for price changes'
            ELSE 'Does not meet investment criteria'
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
    calculation_details,
    score_created_at,
    score_updated_at
FROM final_output
WHERE property_id IS NOT NULL  -- Ensure we have valid properties
ORDER BY humankind_score DESC