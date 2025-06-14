WITH rental_assumptions AS (
    SELECT 
        1800 as monthly_rent_per_unit,  -- $1,800/month per unit default
        0.45 as annual_expense_ratio,   -- 45% expense ratio default
        0.055 as exit_cap_rate,         -- 5.5% cap rate default
        5 as hold_period_years          -- 5 year hold default
),

property_rlv AS (
    SELECT 
        p.id as property_id,
        p.property_address,
        p.city,
        p.state,
        p.number_of_units,
        p.list_price,
        
        -- Get property-specific inputs or use defaults
        COALESCE(pi.purchase_price, p.list_price) as acquisition_price,
        COALESCE(pi.avg_rent_per_unit, ra.monthly_rent_per_unit) as monthly_rent_per_unit,
        COALESCE(pi.opex_ratio, ra.annual_expense_ratio) as annual_expense_ratio,
        ra.exit_cap_rate as exit_cap_rate,  -- Use default since not in inputs
        ra.hold_period_years as hold_period_years,  -- Use default since not in inputs
        
        -- RLV Calculation
        (
            -- Annual NOI
            (p.number_of_units * COALESCE(pi.avg_rent_per_unit, ra.monthly_rent_per_unit) * 12) * 
            (1 - COALESCE(pi.opex_ratio, ra.annual_expense_ratio))
        ) / ra.exit_cap_rate as calculated_rlv,
        
        -- Upside calculation
        (
            (
                (
                    -- Annual NOI
                    (p.number_of_units * COALESCE(pi.avg_rent_per_unit, ra.monthly_rent_per_unit) * 12) * 
                    (1 - COALESCE(pi.opex_ratio, ra.annual_expense_ratio))
                ) / ra.exit_cap_rate
            ) - p.list_price
        ) / p.list_price as upside_percentage
        
    FROM {{ source('costar_analysis', 'raw_properties') }} p
    LEFT JOIN {{ source('inputs', 'property_inputs') }} pi 
        ON p.property_address = pi.property_address
    CROSS JOIN rental_assumptions ra
    WHERE p.list_price > 0 
      AND p.list_price < 50000000  -- Filter out crazy prices (anything over $50M)
      AND p.number_of_units > 0
      AND p.property_address IS NOT NULL  -- Remove any null addresses
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