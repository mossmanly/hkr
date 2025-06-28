-- models/facts/tbl_terms.sql
-- Investment terms with calculated ratios
{{ config(materialized='table') }}

SELECT
    -- Identifiers
    investor_serial,
    portfolio_id,
    
    -- Investor information
    first_name,
    last_name,
    full_name,
    company_name,
    anonymous_name,
    
    -- Investment terms
    equity_class,
    equity_contributed,
    base_pref_irr,
    target_irr,
    
    -- Calculated ratio by portfolio (replaces invested_metrics.percentage_of_investments)
    ROUND(
        equity_contributed / SUM(equity_contributed) OVER (PARTITION BY portfolio_id), 2
    ) AS percentage_of_investments
    
FROM hkh_dev.tbl_terms
WHERE equity_contributed IS NOT NULL
ORDER BY portfolio_id, equity_contributed DESC