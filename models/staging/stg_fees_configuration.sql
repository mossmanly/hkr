{{ config(materialized='view') }}

-- Enhanced fees configuration staging model
-- Sources from existing stg_fees_growth_factors table with base percentages + inflation rates
-- Single source of truth for all professional fee calculations

SELECT
    company_id,
    portfolio_id,
    fee_component,
    
    -- Base percentage of PGI for dynamic calculation
    base_pct_of_pgi,
    
    -- Annual inflation rate for growth modeling
    annual_inflation_rate,
    
    -- Market review metadata
    last_market_review_date,
    market_benchmark_source,
    next_review_due_date,
    variance_from_market,
    review_frequency_months,
    notes,
    
    -- Audit trail
    created_by,
    created_at,
    updated_at

FROM {{ source('inputs', 'stg_fees_growth_factors') }}
WHERE base_pct_of_pgi IS NOT NULL
  AND company_id = 1  -- Company scoping
ORDER BY 
    company_id, 
    COALESCE(portfolio_id, 'company_default'), 
    fee_component