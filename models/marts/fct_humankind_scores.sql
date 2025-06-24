-- models/marts/fct_humankind_scores.sql
-- HumanKindScore™ Calculator Model - NOW WITH CRIME SAFETY INTEGRATION
-- Enhanced with crime data from Phase 1 enrichment

{{ config(
    materialized='table',
    schema='costar_analysis',
    indexes=[
        {'columns': ['analysis_run_id']},
        {'columns': ['humankind_score']},
        {'columns': ['property_id']},
        {'columns': ['crime_score']},
        {'columns': ['safety_adjusted_score']}
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
    FROM costar_analysis.int_costar_rlv_calculator
),

-- Get crime safety data from enhanced crime model  
crime_data AS (
    SELECT 
        property_id,
        crime_score,
        crime_grade,
        detailed_safety_grade,
        crime_safety_tier,
        safety_assessment_workforce,
        safety_risk_flag,
        crime_data_reliability,
        crime_data_source
    FROM costar_analysis.int_property_crime_enhanced
),

-- Get scoring weights from database table (NO HARDCODING!)
scoring_weights_data AS (
    SELECT 
        weight_name,
        weight_percentage
    FROM costar_analysis.scoring_weights 
    WHERE is_active = true
),

-- Pivot weights for easier use - ALL FROM DATABASE
scoring_weights AS (
    SELECT 
        MAX(CASE WHEN weight_name = 'rlv_weight' THEN weight_percentage END) as rlv_weight_pct,
        MAX(CASE WHEN weight_name = 'scale_weight' THEN weight_percentage END) as scale_weight_pct,
        MAX(CASE WHEN weight_name = 'cost_weight' THEN weight_percentage END) as cost_weight_pct,
        MAX(CASE WHEN weight_name = 'location_weight' THEN weight_percentage END) as location_weight_pct,
        MAX(CASE WHEN weight_name = 'crime_weight' THEN weight_percentage END) as crime_weight_pct,
        MAX(CASE WHEN weight_name = 'crime_fallback_score' THEN weight_percentage ELSE 50.0 END) as crime_fallback_score
    FROM scoring_weights_data
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
        
        -- NEW: Crime safety data
        COALESCE(cd.crime_score, sw.crime_fallback_score) as crime_score,
        COALESCE(cd.crime_grade, 'C') as crime_grade,
        COALESCE(cd.detailed_safety_grade, 'C Moderate') as detailed_safety_grade,
        COALESCE(cd.crime_safety_tier, 'Moderate') as crime_safety_tier,
        COALESCE(cd.safety_assessment_workforce, 'Unknown Safety Profile') as safety_assessment_workforce,
        COALESCE(cd.safety_risk_flag, 'Medium Risk - No Safety Data') as safety_risk_flag,
        COALESCE(cd.crime_data_reliability, 'No Data') as crime_data_reliability,
        COALESCE(cd.crime_data_source, 'no_data') as crime_data_source,
        
        -- Scoring weights (now includes crime)
        sw.rlv_weight_pct,
        sw.scale_weight_pct,
        sw.cost_weight_pct,
        sw.location_weight_pct,
        sw.crime_weight_pct,
        sw.crime_fallback_score
        
    FROM raw_properties rp
    LEFT JOIN rlv_data rlv ON rp.id = rlv.property_id
    LEFT JOIN crime_data cd ON rp.id = cd.property_id
    CROSS JOIN scoring_weights sw
),

scoring AS (
    SELECT *,
        
        -- 1. RLV Score (Database-driven weight)
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
        
        -- 2. Scale Score (20% weight - UNCHANGED)  
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
        
        -- 3. Cost Efficiency Score (15% weight - UNCHANGED)
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
        
        -- 4. Location Score (15% weight - UNCHANGED)
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
        
        -- 5. NEW: Crime Safety Score (10% weight)
        CASE 
            WHEN crime_score >= 90 THEN (50 * crime_weight_pct)       -- Exceptional safety
            WHEN crime_score >= 80 THEN (45 * crime_weight_pct)       -- Excellent safety
            WHEN crime_score >= 70 THEN (40 * crime_weight_pct)       -- Very good safety
            WHEN crime_score >= 60 THEN (35 * crime_weight_pct)       -- Good safety
            WHEN crime_score >= 50 THEN (30 * crime_weight_pct)       -- Moderate safety
            WHEN crime_score >= 40 THEN (20 * crime_weight_pct)       -- Below average safety
            WHEN crime_score >= 30 THEN (10 * crime_weight_pct)       -- Poor safety
            ELSE (5 * crime_weight_pct)   -- Very poor safety
        END as crime_safety_score,
        
        CASE 
            WHEN crime_score >= 90 THEN 'Exceptional Safety (90+) - Premium workforce location'
            WHEN crime_score >= 80 THEN 'Excellent Safety (80-89) - Very safe for workforce housing'
            WHEN crime_score >= 70 THEN 'Very Good Safety (70-79) - Safe workforce housing'
            WHEN crime_score >= 60 THEN 'Good Safety (60-69) - Suitable for workforce housing'
            WHEN crime_score >= 50 THEN 'Moderate Safety (50-59) - Standard workforce housing'
            WHEN crime_score >= 40 THEN 'Below Average Safety (40-49) - May need security measures'
            WHEN crime_score >= 30 THEN 'Poor Safety (30-39) - Significant safety concerns'
            ELSE 'Very Poor Safety (<30) - Not recommended for workforce housing'
        END as crime_safety_reasoning,
        
        -- 6. Placeholder scores for future enrichment (UNCHANGED)
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
        
        -- Calculate total HumanKindScore (NOW INCLUDES CRIME!)
        (rlv_score + scale_score + cost_efficiency_score + location_score + crime_safety_score +
         COALESCE(school_quality_score, 0) + 
         COALESCE(healthcare_access_score, 0) + 
         COALESCE(transit_walkability_score, 0)) as humankind_score,
         
        -- NEW: Safety-adjusted score for workforce housing focus
        ROUND(
            (rlv_score + scale_score + cost_efficiency_score + location_score + crime_safety_score +
             COALESCE(school_quality_score, 0) + 
             COALESCE(healthcare_access_score, 0) + 
             COALESCE(transit_walkability_score, 0)) *
            -- Apply safety multiplier for workforce housing appropriateness
            CASE 
                WHEN crime_score >= 70 THEN 1.05      -- Safety bonus for workforce housing
                WHEN crime_score >= 55 THEN 1.00      -- Neutral
                WHEN crime_score >= 40 THEN 0.95      -- Small penalty for safety concerns
                ELSE 0.85  -- Larger penalty for poor safety
            END, 2
        ) as safety_adjusted_score,
         
        -- Score tier classification (updated for new max score)
        CASE 
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + crime_safety_score +
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 45 THEN 'Excellent'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + crime_safety_score +
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 35 THEN 'Good'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + crime_safety_score +
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 25 THEN 'Fair'
            ELSE 'Poor'
        END as score_tier,
        
        -- Investment recommendation (now with crime and safety considerations)
        CASE 
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + crime_safety_score +
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 40 AND crime_score >= 55 THEN 'Strong Buy'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + crime_safety_score +
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 35 AND crime_score >= 45 THEN 'Consider'
            WHEN (rlv_score + scale_score + cost_efficiency_score + location_score + crime_safety_score +
                  COALESCE(school_quality_score, 0) + 
                  COALESCE(healthcare_access_score, 0) + 
                  COALESCE(transit_walkability_score, 0)) >= 25 THEN 'Watch'
            WHEN crime_score < 35 THEN 'Pass - Safety Concerns'
            ELSE 'Pass'
        END as investment_recommendation,
        
        -- Methodology version (updated)
        'v2.1_with_crime_safety' as methodology_version,
        
        -- Calculation timestamp
        CURRENT_TIMESTAMP as calculated_at,
        
        -- Updated metadata fields
        100 as total_possible_score,  -- Full scoring potential
        55 as current_max_score,      -- All current components max (includes crime)
        45 as enrichment_pending      -- Future enrichment data pending
        
    FROM scoring
),

final_output AS (
    SELECT *,
        CASE 
            WHEN investment_recommendation = 'Strong Buy' THEN 
                'Exceptional opportunity (Score: ' || ROUND(humankind_score, 1) || ', Safety: ' || crime_grade || ') - Strong RLV upside with excellent safety for workforce housing'
            WHEN investment_recommendation = 'Consider' THEN 
                'Good opportunity (Score: ' || ROUND(humankind_score, 1) || ', Safety: ' || crime_grade || ') - Solid metrics with adequate safety for workforce housing'
            WHEN investment_recommendation = 'Watch' THEN 
                'Monitor opportunity (Score: ' || ROUND(humankind_score, 1) || ', Safety: ' || crime_grade || ') - Some positives, watch for improvements'
            WHEN investment_recommendation = 'Pass - Safety Concerns' THEN
                'Below safety threshold (Score: ' || ROUND(humankind_score, 1) || ', Safety: ' || crime_grade || ') - Safety concerns for workforce housing'
            ELSE 'Below target criteria (Score: ' || ROUND(humankind_score, 1) || ', Safety: ' || crime_grade || ') - Does not meet investment thresholds'
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
    -- RLV components (existing)
    calculated_rlv,
    upside_percentage,
    upside_category,
    rlv_score,
    rlv_reasoning,
    -- Other scoring components (existing)
    location_score,
    location_reasoning,
    scale_score,
    scale_reasoning,
    cost_efficiency_score,
    cost_efficiency_reasoning,
    -- NEW: Crime safety components
    crime_score,
    crime_grade,
    detailed_safety_grade,
    crime_safety_tier,
    crime_safety_score,
    crime_safety_reasoning,
    safety_assessment_workforce,
    safety_risk_flag,
    crime_data_reliability,
    crime_data_source,
    -- Future enrichment (existing placeholders)
    school_quality_score,
    school_quality_reasoning,
    healthcare_access_score,
    healthcare_reasoning,
    transit_walkability_score,
    transit_reasoning,
    -- Final scores (enhanced)
    humankind_score,
    safety_adjusted_score,  -- NEW: Safety-adjusted score for workforce housing
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
ORDER BY safety_adjusted_score DESC, humankind_score DESC