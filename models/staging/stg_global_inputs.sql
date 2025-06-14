{{ config(materialized='view') }}

-- Staging model for global inputs and capex assumptions
-- Clean view of system-wide parameters for modeling and calculations
-- Source: inputs.global_inputs

SELECT
    id,
    company_id,
    portfolio_id,
    ltl_days_reno,
    ltl_days_norm,
    capex_float_interest_rate,
    capex_float_description,
    
    -- Portfolio context fields
    company_id as scoping_company_id,
    portfolio_id as scoping_portfolio_id,
    
    -- Standardize numeric fields
    CAST(capex_float_interest_rate AS DECIMAL(8,4)) as capex_float_rate_clean,
    
    -- Convert days to more useful time periods
    ROUND(ltl_days_reno / 7.0, 1) as ltl_weeks_reno,
    ROUND(ltl_days_norm / 7.0, 1) as ltl_weeks_norm,
    ROUND(ltl_days_reno / 30.0, 1) as ltl_months_reno,
    ROUND(ltl_days_norm / 30.0, 1) as ltl_months_norm,
    
    -- Add input categories for better organization
    CASE
        WHEN capex_float_interest_rate > 0 THEN 'interest_bearing'
        ELSE 'non_interest_bearing'
    END as float_account_type,
    
    -- Add validation flags for ltl (lease to lease) timing
    CASE
        WHEN ltl_days_reno IS NULL OR ltl_days_norm IS NULL THEN 'missing_ltl_data'
        WHEN ltl_days_reno < 0 OR ltl_days_norm < 0 THEN 'negative_days'
        WHEN ltl_days_reno > 365 OR ltl_days_norm > 365 THEN 'excessive_days'
        WHEN ltl_days_reno < ltl_days_norm THEN 'reno_faster_than_norm'
        ELSE 'valid'
    END as ltl_validation_status,
    
    -- Interest rate validation
    CASE
        WHEN capex_float_interest_rate IS NULL THEN 'missing_rate'
        WHEN capex_float_interest_rate < 0 THEN 'negative_rate'
        WHEN capex_float_interest_rate > 0.15 THEN 'high_rate'
        ELSE 'valid'
    END as interest_rate_validation,
    
    -- Portfolio scoping validation (should match portfolio_settings)
    CASE
        WHEN company_id = 1 THEN 'hkh_scoped'
        ELSE 'other_company'
    END as company_scope,
    
    -- Description cleanup
    COALESCE(TRIM(capex_float_description), 'No description provided') as capex_float_description_clean,
    
    -- Add metadata
    CURRENT_TIMESTAMP as staging_loaded_at

FROM {{ source('inputs', 'global_inputs') }}

-- This staging model provides clean capex and timing parameters
-- LTL = Lease to Lease (vacancy period between tenants)
-- Float rate = Interest earned on capex reserve funds