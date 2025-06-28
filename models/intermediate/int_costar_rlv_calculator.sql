--models/int_costar_rlv_calculator.sql

{{ config(
    materialized='view',
    schema='costar_analysis'
) }}

WITH market_parameters AS (
    SELECT 
        MAX(CASE WHEN parameter_name = 'default_monthly_rent_per_unit' THEN parameter_value END) as default_monthly_rent,
        MAX(CASE WHEN parameter_name = 'hold_period_years' THEN parameter_value END) as hold_period_years,
        MAX(CASE WHEN parameter_name = 'max_property_price' THEN parameter_value END) as max_property_price,
        MAX(CASE WHEN parameter_name = 'min_property_units' THEN parameter_value END) as min_property_units
    FROM hkh_dev.stg_market_parameters
),

market_assumptions AS (
    SELECT 
        -- Get assumptions from your existing model with proper defaults
        COALESCE(standard_vacancy_rate, 0.05) as vacancy_rate,
        COALESCE(standard_opex_ratio, 0.45) as expense_ratio,
        COALESCE(standard_collections_loss, 0.02) as collections_loss,
        COALESCE(market_cap_rate, 0.055) as cap_rate
    FROM {{ ref('int_rlv_assumptions') }}
    LIMIT 1
),

combined_assumptions AS (
    SELECT 
        ma.*,
        mp.default_monthly_rent,
        mp.hold_period_years,
        mp.max_property_price,
        mp.min_property_units
    FROM market_assumptions ma
    CROSS JOIN market_parameters mp
),

property_rlv AS (
    SELECT 
        p.id as property_id,
        p.property_address,
        p.city,
        p.state,
        p.number_of_units,
        p.list_price,
        
        -- Get property-specific inputs or use market assumptions
        COALESCE(pi.purchase_price, p.list_price) as acquisition_price,
        COALESCE(pi.avg_rent_per_unit, ca.default_monthly_rent) as monthly_rent_per_unit,
        COALESCE(pi.opex_ratio, ca.expense_ratio) as annual_expense_ratio,
        COALESCE(pi.vacancy_rate, ca.vacancy_rate) as vacancy_rate,
        COALESCE(pi.collections_loss_rate, ca.collections_loss) as collections_loss,
        ca.cap_rate as exit_cap_rate,
        ca.hold_period_years as hold_period_years,
        
        -- RLV Calculation with proper vacancy and collections adjustments
        (
            -- Effective Annual NOI
            (p.number_of_units * COALESCE(pi.avg_rent_per_unit, ca.default_monthly_rent) * 12) * 
            (1 - COALESCE(pi.vacancy_rate, ca.vacancy_rate)) *
            (1 - COALESCE(pi.collections_loss_rate, ca.collections_loss)) *
            (1 - COALESCE(pi.opex_ratio, ca.expense_ratio))
        ) / ca.cap_rate as calculated_rlv,
        
        -- Upside calculation
        (
            (
                (
                    -- Effective Annual NOI
                    (p.number_of_units * COALESCE(pi.avg_rent_per_unit, ca.default_monthly_rent) * 12) * 
                    (1 - COALESCE(pi.vacancy_rate, ca.vacancy_rate)) *
                    (1 - COALESCE(pi.collections_loss_rate, ca.collections_loss)) *
                    (1 - COALESCE(pi.opex_ratio, ca.expense_ratio))
                ) / ca.cap_rate
            ) - p.list_price
        ) / p.list_price as upside_percentage
        
    FROM {{ source('costar_analysis', 'raw_properties') }} p
    LEFT JOIN hkh_dev.stg_property_inputs pi 
        ON p.property_address = pi.property_address
    CROSS JOIN combined_assumptions ca
    WHERE p.list_price > 0 
      AND p.list_price < ca.max_property_price
      AND p.number_of_units >= ca.min_property_units
      AND p.property_address IS NOT NULL
)

SELECT 
    *,
    -- Upside category
    CASE 
        WHEN upside_percentage >= 0.50 THEN 'Exceptional (50%+)'
        WHEN upside_percentage >= 0.25 THEN 'Excellent (25-50%)'
        WHEN upside_percentage >= 0.10 THEN 'Good (10-25%)'
        WHEN upside_percentage >= 0.0 THEN 'Break Even (0-10%)'
        ELSE 'Overpriced'
    END as upside_category
    
FROM property_rlv
ORDER BY upside_percentage DESC