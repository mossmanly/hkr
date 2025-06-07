{{ config(materialized='view') }}

SELECT
    investor_serial,
    COALESCE(first_name, 'Personal Loan') AS first_name,
    COALESCE(last_name, investor_serial) AS last_name,
    equity_contributed,
    ROUND(
        (equity_contributed * 100.0) / SUM(equity_contributed) OVER (), 2
        ) AS percentage_of_investments

FROM hkh_dev.tbl_terms
WHERE equity_contributed IS NOT NULL
ORDER BY equity_contributed DESC

-- KEY FIXES EXPLAINED:
-- 1. Fixed table reference: hkh_dev.tbl_terms (not {{'hkh_dev','tbl_terms'}})
-- 2. Used window function SUM() OVER() instead of subquery
-- 3. Added missing columns (investor info)
-- 4. Fixed column name: equity_contributed (not equity_invested)
-- 5. Removed GROUP BY (not needed with window function)
-- 6. Added ORDER BY for readability
-- 7. Used ROUND() for clean percentages
-- 8. Added COALESCE() to handle NULL names for personal loans
-- 9. Added WHERE clause to ensure only real contributions are included

-- COMMON dbt/SQL PATTERNS FOR YOUR TOOLKIT:

-- Pattern 1: Window functions for percentages
-- column / SUM(column) OVER() * 100

-- Pattern 2: Table references in dbt
-- schema.table_name (no curly braces for direct references)

-- Pattern 3: Percentage calculations
-- Always multiply by 100.0 (not 100) to avoid integer division

-- Pattern 4: GROUP BY rule
-- If you SELECT non-aggregated columns, you must GROUP BY them
-- OR use window functions to avoid grouping

-- Pattern 5: Handling NULLs
-- Use COALESCE(column, 'default_value') to replace NULLs with meaningful values