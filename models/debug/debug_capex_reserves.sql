-- models/debug_capex_reserves.sql
-- SIMPLIFIED DEBUG VERSION: Let's see what's happening step by step

{{ config(materialized='view') }}

-- Test 1: Just get property inputs
WITH step1_properties AS (
    SELECT 
        property_id,
        unit_count,
        capex_per_unit,
        unit_count * capex_per_unit AS total_reserves_raised
    FROM inputs.property_inputs
),

-- Test 2: Just get capex factors
step2_capex_factors AS (
    SELECT 
        property_id,
        year,
        capex_factor
    FROM inputs.capex_factors
),

-- Test 3: Try the join
step3_joined AS (
    SELECT 
        cf.property_id,
        cf.year,
        cf.capex_factor,
        sp.unit_count,
        sp.capex_per_unit,
        sp.total_reserves_raised,
        ROUND(sp.unit_count * sp.capex_per_unit * cf.capex_factor, 0) AS capex_spent
    FROM step2_capex_factors cf
    JOIN step1_properties sp ON sp.property_id = cf.property_id
)

-- Just return the basic joined data for now
SELECT 
    property_id,
    year,
    unit_count,
    capex_per_unit,
    total_reserves_raised,
    capex_factor,
    capex_spent,
    'debug_step_3' as debug_info
FROM step3_joined
ORDER BY property_id, year