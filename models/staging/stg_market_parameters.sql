{{ config(materialized='view') }}

-- Staging model for market parameters
-- Clean view of market-level assumptions for RLV calculations and modeling
-- Source: inputs.market_parameters

SELECT
    id as parameter_id,
    parameter_name,
    parameter_value,
    parameter_description,
    effective_date,
    created_by,
    created_at,
    updated_at,
    
    -- Standardize parameter names for consistency
    LOWER(TRIM(parameter_name)) as parameter_name_clean,
    
    -- Cast parameter value to ensure numeric consistency
    CAST(parameter_value AS DECIMAL(10,4)) as parameter_value_numeric,
    
    -- Add parameter categories for better organization
    CASE
        WHEN LOWER(parameter_name) LIKE '%rate%' OR LOWER(parameter_name) LIKE '%yield%' THEN 'rate'
        WHEN LOWER(parameter_name) LIKE '%price%' OR LOWER(parameter_name) LIKE '%cost%' THEN 'pricing'
        WHEN LOWER(parameter_name) LIKE '%factor%' OR LOWER(parameter_name) LIKE '%multiplier%' THEN 'factor'
        WHEN LOWER(parameter_name) LIKE '%inflation%' OR LOWER(parameter_name) LIKE '%appreciation%' THEN 'growth'
        WHEN LOWER(parameter_name) LIKE '%cap%' THEN 'cap_rate'
        ELSE 'other'
    END as parameter_category,
    
    -- Add validation flags
    CASE
        WHEN parameter_value IS NULL THEN 'missing_value'
        WHEN parameter_value < 0 AND parameter_name NOT LIKE '%depreciation%' THEN 'negative_value'
        WHEN parameter_value > 1 AND LOWER(parameter_name) LIKE '%rate%' THEN 'high_rate'
        ELSE 'valid'
    END as validation_status,
    
    -- Check if parameter is currently effective
    CASE
        WHEN effective_date IS NULL THEN TRUE
        WHEN effective_date <= CURRENT_DATE THEN TRUE
        ELSE FALSE
    END as is_currently_effective,
    
    -- Add metadata
    CURRENT_TIMESTAMP as staging_loaded_at

FROM {{ source('inputs', 'market_parameters') }}

-- This staging model provides clean, categorized market parameters
-- for use in RLV calculations and property modeling