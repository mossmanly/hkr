{{ config(materialized='table') }}

/*
    Core dimension table for real estate operational years
    
    Generates years 1-20 for real estate financial modeling where:
    - Year 0 = Development/construction period (not included)
    - Year 1 = First year of stabilized operations  
    - Year 2+ = Subsequent operational years
    
    year_offset provides 0-based indexing helpful for compounding calculations
*/

WITH years AS (
    SELECT generate_series(1, 20) AS year
)

SELECT 
    year,
    'Year ' || year AS year_label,
    year - 1 AS year_offset  -- helpful for compounding
FROM years