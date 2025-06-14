{{ config(materialized='view') }}

-- Staging model for property inputs
-- Clean, standardized view of core property characteristics and acquisition assumptions
-- Source: inputs.property_inputs

SELECT
    company_id,
    portfolio_id,
    property_id,
    acquisition_year,
    unit_count,
    avg_rent_per_unit,
    init_turn_rate,
    norm_turn_rate,
    cola_snap,
    norm_snap,
    reno_snap,
    mtm_snap,
    vacancy_rate,
    property_address,
    property_name,
    city,
    zip,
    building_class,
    collections_loss_rate,
    opex_ratio,
    list_price,
    rlv_price,
    purchase_price,
    ds_ltv,
    ds_term,
    ds_int,
    ds_refi_ltv,
    ds_refi_term,
    ds_refi_int,
    ds_refi_year,
    
    -- Add clean calculated fields for downstream use
    ROUND(avg_rent_per_unit, 2) as monthly_rent_per_unit_clean,
    ROUND(purchase_price, 2) as purchase_price_clean,
    ROUND(list_price, 2) as list_price_clean,
    ROUND(rlv_price, 2) as rlv_price_clean,
    
    -- Calculate key metrics
    unit_count * avg_rent_per_unit * 12 as gross_annual_income,
    CASE 
        WHEN unit_count > 0 THEN purchase_price / unit_count 
        ELSE NULL 
    END as price_per_unit,
    
    -- Debt service calculations
    purchase_price * (ds_ltv / 100.0) as initial_loan_amount,
    
    -- Add metadata
    CURRENT_TIMESTAMP as staging_loaded_at

FROM {{ source('inputs', 'property_inputs') }}

-- This staging model maintains all source data while adding clean, calculated fields
-- All downstream models should reference this staging model, not the raw source 