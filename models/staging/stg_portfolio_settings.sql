{{ config(materialized='view') }}

-- Staging model for portfolio settings and configuration
-- Clean view of portfolio management and multi-tenancy settings
-- Source: inputs.portfolio_settings
-- CRITICAL: This table drives portfolio filtering throughout the system

SELECT
    id,
    company_id,
    portfolio_id,
    portfolio_name,
    portfolio_description,
    is_default,
    is_active,
    investment_strategy,
    target_property_count,
    target_total_units,
    created_at,
    updated_at,
    
    -- Standardize boolean fields for consistency
    CASE 
        WHEN is_default IS TRUE THEN TRUE 
        ELSE FALSE 
    END as is_default_portfolio,
    
    CASE 
        WHEN is_active IS TRUE THEN TRUE 
        ELSE FALSE 
    END as is_active_portfolio,
    
    -- Add portfolio status classification
    CASE
        WHEN is_active = TRUE AND is_default = TRUE THEN 'active_default'
        WHEN is_active = TRUE AND is_default = FALSE THEN 'active_secondary'
        WHEN is_active = FALSE THEN 'inactive'
        ELSE 'unknown'
    END as portfolio_status,
    
    -- Add company scoping for multi-tenancy
    -- Currently expecting company_id = 1 for HKH
    CASE
        WHEN company_id = 1 THEN 'hkh_primary'
        ELSE 'other_company'
    END as company_scope,
    
    -- Investment strategy classification
    COALESCE(LOWER(TRIM(investment_strategy)), 'unspecified') as investment_strategy_clean,
    
    -- Add metadata
    CURRENT_TIMESTAMP as staging_loaded_at

FROM {{ source('inputs', 'portfolio_settings') }}

-- Portfolio filtering pattern for downstream models:
-- WHERE company_id = 1 AND is_default = TRUE
-- This staging model standardizes these critical filtering fields  