-- models/intermediate/int_rlv_assumptions.sql
-- 
-- This model extracts average operating assumptions from your existing 4 properties
-- These assumptions will be applied to CoStar data to calculate RLV for each property
--
{{ config(
    materialized='view',
    schema='costar_analysis'
) }}

WITH scoring_weights AS (
    SELECT 
        MAX(CASE WHEN parameter_name = 'rlv_weight_pct' THEN parameter_value END) as rlv_weight_pct,
        MAX(CASE WHEN parameter_name = 'scale_weight_pct' THEN parameter_value END) as scale_weight_pct,
        MAX(CASE WHEN parameter_name = 'cost_weight_pct' THEN parameter_value END) as cost_weight_pct,
        MAX(CASE WHEN parameter_name = 'location_weight_pct' THEN parameter_value END) as location_weight_pct
    FROM hkh_dev.stg_market_parameters
)

select 
    -- Scoring weight configuration (now from market_parameters instead of hardcoded)
    MAX(sw.rlv_weight_pct) as rlv_weight_pct,           -- RLV gets 50% of total score
    MAX(sw.scale_weight_pct) as scale_weight_pct,         -- Unit scale gets 20%
    MAX(sw.cost_weight_pct) as cost_weight_pct,          -- Cost efficiency gets 15% 
    MAX(sw.location_weight_pct) as location_weight_pct,      -- Location gets 15%
    
    -- Operating assumptions from your existing 4 properties
    round(avg(vacancy_rate), 4) as standard_vacancy_rate,
    round(avg(opex_ratio), 4) as standard_opex_ratio,
    round(avg(collections_loss_rate), 4) as standard_collections_loss,
    
    -- Financing assumptions 
    round(avg(ds_ltv), 4) as standard_loan_ltv,
    round(avg(ds_int), 4) as standard_interest_rate,
    round(avg(ds_term), 0) as standard_loan_term,
    
    -- Market cap rate derived from your existing deals
    round(avg(
        case 
            when rlv_price > 0 and list_price > 0 
            then (avg_rent_per_unit * unit_count * 12 * (1 - vacancy_rate) * (1 - opex_ratio)) / rlv_price
            else null
        end
    ), 4) as market_cap_rate,
    
    -- Validation metrics
    count(*) as assumption_property_count,
    round(avg(avg_rent_per_unit), 2) as avg_rent_check,
    
    current_timestamp as calculated_at
    
from hkh_dev.stg_property_inputs pi
CROSS JOIN scoring_weights sw
where rlv_price is not null 
    and avg_rent_per_unit is not null
    and vacancy_rate is not null
    and opex_ratio is not null