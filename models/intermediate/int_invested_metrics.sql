{{ config(materialized='view') }}

/*
    Investor equity contribution analysis with percentage calculations
    
    Calculates each investor's percentage of total equity contributions
    using window functions for accurate portfolio-level percentages.
    
    Migration Notes:
    - Migrated from root level to intermediate layer
    - Updated reference: hkh_dev.tbl_terms → direct table reference
    - Preserved exact business logic for equity percentage calculations
    - Maintained COALESCE logic for handling personal loans
    - Verified all required columns exist in stg_terms
*/

SELECT
    investor_serial,
    COALESCE(first_name, 'Personal Loan') AS first_name,
    COALESCE(last_name, investor_serial) AS last_name,
    equity_contributed,
    ROUND(
        (equity_contributed * 100.0) / SUM(equity_contributed) OVER (), 2
        ) AS percentage_of_investments

FROM hkh_dev.stg_terms
WHERE equity_contributed IS NOT NULL
ORDER BY equity_contributed DESC 