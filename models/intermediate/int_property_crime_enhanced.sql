-- Property Crime Enhancement Intermediate Model
-- Joins crime safety data to properties with business logic

{{ config(
    materialized='view',
    schema='costar_analysis'
) }}

WITH raw_properties AS (
    SELECT * FROM costar_analysis.raw_properties
    WHERE (not_ranked_reason IS NULL OR not_ranked_reason = '')
      AND number_of_units > 0
      AND list_price > 0
      AND price_per_unit > 0
),

crime_data AS (
    SELECT * FROM {{ ref('stg_external_crime_data') }}
),

property_crime_joined AS (
    SELECT 
        -- Property identifiers
        rp.id as property_id,
        rp.upload_batch_id,
        rp.costar_property_id,
        
        -- Property details
        rp.property_address,
        rp.property_name,
        rp.city,
        rp.state,
        rp.zip_code,
        rp.county_name,
        rp.market_name,
        rp.submarket_name,
        
        -- Property characteristics
        rp.number_of_units,
        rp.list_price,
        rp.price_per_unit,
        rp.building_class,
        rp.building_status,
        rp.year_built,
        rp.year_renovated,
        rp.property_style,
        rp.rba_sqft,
        rp.star_rating,
        rp.energy_star,
        rp.leed_certified,
        
        -- Financial data
        rp.rlv_price,
        rp.first_seen_date,
        rp.last_seen_date,
        rp.times_imported,
        
        -- Crime safety data (with fallbacks)
        COALESCE(cd.crime_score, 50) as crime_score,  -- Default to neutral 50 if no data
        COALESCE(cd.crime_grade, 'C') as crime_grade,
        COALESCE(cd.crime_safety_tier, 'Moderate') as crime_safety_tier,
        COALESCE(cd.data_source, 'no_data') as crime_data_source,
        COALESCE(cd.data_quality_flag, 'no_data') as crime_data_quality,
        cd.api_response_code as crime_api_response,
        cd.last_updated as crime_data_updated,
        
        -- Metadata
        rp.uploaded_at,
        rp.uploaded_by,
        rp.created_at,
        rp.updated_at
        
    FROM raw_properties rp
    LEFT JOIN crime_data cd ON rp.id = cd.property_id
),

enhanced_with_business_logic AS (
    SELECT *,
        
        -- Crime score validation and adjustment
        CASE 
            WHEN crime_score < 0 THEN 0
            WHEN crime_score > 100 THEN 100
            ELSE crime_score
        END as validated_crime_score,
        
        -- Data quality assessment
        CASE 
            WHEN crime_data_quality = 'primary_source' THEN 'High'
            WHEN crime_data_quality = 'zip_fallback' THEN 'Medium'
            WHEN crime_data_quality = 'county_fallback' THEN 'Medium'
            WHEN crime_data_quality = 'estimated_data' THEN 'Low'
            WHEN crime_data_quality = 'state_median' THEN 'Low'
            ELSE 'No Data'
        END as crime_data_reliability,
        
        -- Safety score interpretation for workforce housing
        CASE 
            WHEN crime_score >= 85 THEN 'Excellent Safety - Premium workforce housing location'
            WHEN crime_score >= 70 THEN 'Good Safety - Suitable for workforce housing'
            WHEN crime_score >= 55 THEN 'Moderate Safety - Standard workforce housing'
            WHEN crime_score >= 40 THEN 'Below Average Safety - May require additional security measures'
            ELSE 'Poor Safety - Not recommended for workforce housing'
        END as safety_assessment_workforce,
        
        -- Investment risk flag based on safety
        CASE 
            WHEN crime_score < 40 THEN 'High Risk - Safety Concerns'
            WHEN crime_score < 55 AND crime_data_quality = 'estimated_data' THEN 'Medium Risk - Data Quality + Safety'
            WHEN crime_score < 55 THEN 'Medium Risk - Safety'
            WHEN crime_data_quality = 'no_data' THEN 'Medium Risk - No Safety Data'
            ELSE 'Low Risk - Safety'
        END as safety_risk_flag,
        
        -- Score tier refinement for better granularity
        CASE 
            WHEN crime_score >= 90 THEN 'A+ Exceptional'
            WHEN crime_score >= 85 THEN 'A Excellent' 
            WHEN crime_score >= 75 THEN 'B+ Very Good'
            WHEN crime_score >= 65 THEN 'B Good'
            WHEN crime_score >= 55 THEN 'C+ Fair'
            WHEN crime_score >= 45 THEN 'C Moderate'
            WHEN crime_score >= 35 THEN 'D+ Below Average'
            ELSE 'D Poor'
        END as detailed_safety_grade,
        
        -- Days since crime data update
        CASE 
            WHEN crime_data_updated IS NOT NULL THEN 
                EXTRACT(day FROM (CURRENT_TIMESTAMP - crime_data_updated))
            ELSE NULL
        END as crime_data_age_days,
        
        -- Data freshness flag
        CASE 
            WHEN crime_data_updated IS NULL THEN 'No Data'
            WHEN EXTRACT(day FROM (CURRENT_TIMESTAMP - crime_data_updated)) <= 30 THEN 'Fresh'
            WHEN EXTRACT(day FROM (CURRENT_TIMESTAMP - crime_data_updated)) <= 90 THEN 'Recent'
            ELSE 'Stale'
        END as crime_data_freshness
        
    FROM property_crime_joined
)

SELECT 
    -- Property identifiers
    property_id,
    upload_batch_id,
    costar_property_id,
    
    -- Property details
    property_address,
    property_name,
    city,
    state,
    zip_code,
    county_name,
    market_name,
    submarket_name,
    
    -- Property characteristics  
    number_of_units,
    list_price,
    price_per_unit,
    building_class,
    building_status,
    year_built,
    year_renovated,
    property_style,
    rba_sqft,
    star_rating,
    energy_star,
    leed_certified,
    
    -- Financial data
    rlv_price,
    first_seen_date,
    last_seen_date,
    times_imported,
    
    -- Crime safety metrics (validated and enhanced)
    validated_crime_score as crime_score,
    crime_grade,
    detailed_safety_grade,
    crime_safety_tier,
    safety_assessment_workforce,
    safety_risk_flag,
    
    -- Data quality and metadata
    crime_data_source,
    crime_data_quality,
    crime_data_reliability,
    crime_api_response,
    crime_data_updated,
    crime_data_age_days,
    crime_data_freshness,
    
    -- Original timestamps
    uploaded_at,
    uploaded_by,
    created_at,
    updated_at,
    
    -- Model metadata
    CURRENT_TIMESTAMP as enhanced_at
    
FROM enhanced_with_business_logic
ORDER BY validated_crime_score DESC, property_id 