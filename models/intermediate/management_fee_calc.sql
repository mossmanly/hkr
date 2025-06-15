-- models/intermediate/int_management_fee_calc.sql
-- MANAGEMENT FEE LOGIC: Dynamic sponsor promote checking

{{ config(materialized='table') }}

WITH 
-- Global management fee configuration
management_fee_config AS (
    SELECT
        100000 AS annual_management_fee,        -- $100K total fee (configurable)
        0.30 AS opex_allocation_pct,           -- 30% to operations management
        0.70 AS capex_allocation_pct           -- 70% to capex project management
),

-- Get sponsor distribution data from waterfall
sponsor_distributions AS (
    SELECT 
        portfolio_id,
        year,
        total_sponsor 
    FROM hkh_dev.fact_portfolio_waterfall
),

-- Determine cutoff year (first year sponsor promote >= management fee)
sponsor_cutoff AS (
    SELECT
        sd.portfolio_id,
        mfc.annual_management_fee,
        
        -- Find first year where sponsor promote exceeds management fee
        MIN(CASE WHEN sd.total_sponsor >= mfc.annual_management_fee THEN sd.year ELSE NULL END) AS cutoff_year
        
    FROM sponsor_distributions sd
    CROSS JOIN management_fee_config mfc
    GROUP BY sd.portfolio_id, mfc.annual_management_fee
),

-- Generate year range for all portfolios
year_range AS (
    SELECT DISTINCT
        portfolio_id,
        year
    FROM sponsor_distributions
),

-- Calculate management fees with dynamic cutoff
management_fee_schedule AS (
    SELECT
        yr.portfolio_id,
        yr.year,
        mfc.annual_management_fee,
        mfc.opex_allocation_pct,
        mfc.capex_allocation_pct,
        sc.cutoff_year,
        COALESCE(sd.total_sponsor, 0) AS sponsor_promote_this_year,
        
        -- Management fee eligibility logic
        CASE 
            WHEN sc.cutoff_year IS NULL THEN 1                    -- No cutoff yet, fees continue
            WHEN yr.year < sc.cutoff_year THEN 1                  -- Before cutoff year, fees active
            ELSE 0                                                -- At or after cutoff, no fees
        END AS fee_eligible,
        
        -- Calculate actual management fee amounts
        CASE 
            WHEN sc.cutoff_year IS NULL OR yr.year < sc.cutoff_year 
            THEN ROUND(mfc.annual_management_fee * mfc.opex_allocation_pct, 0)
            ELSE 0
        END AS management_fee_opex,
        
        CASE 
            WHEN sc.cutoff_year IS NULL OR yr.year < sc.cutoff_year 
            THEN ROUND(mfc.annual_management_fee * mfc.capex_allocation_pct, 0)
            ELSE 0
        END AS management_fee_capex
        
    FROM year_range yr
    CROSS JOIN management_fee_config mfc
    LEFT JOIN sponsor_cutoff sc ON yr.portfolio_id = sc.portfolio_id
    LEFT JOIN sponsor_distributions sd ON yr.portfolio_id = sd.portfolio_id AND yr.year = sd.year
)

-- Final output with metadata for debugging
SELECT
    portfolio_id,
    year,
    management_fee_opex,
    management_fee_capex,
    management_fee_opex + management_fee_capex AS total_management_fee,
    
    -- Debugging fields
    fee_eligible,
    cutoff_year,
    sponsor_promote_this_year,
    annual_management_fee AS config_total_fee,
    
    -- Status description for easy understanding
    CASE 
        WHEN cutoff_year IS NULL THEN 'Active - No sponsor promote threshold reached'
        WHEN year < cutoff_year THEN 'Active - Before sponsor promote threshold'
        WHEN year = cutoff_year THEN 'CUTOFF YEAR - Sponsor promote exceeded threshold'
        ELSE 'Inactive - Post sponsor promote threshold'
    END AS fee_status
    
FROM management_fee_schedule
ORDER BY portfolio_id, year