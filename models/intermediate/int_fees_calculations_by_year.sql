{{ config(materialized='table') }}

-- Multi-year professional fees projections with component-level inflation
-- Now uses dynamic base fees from configuration instead of hard-coded amounts

WITH years AS (
    SELECT generate_series(1, 20) AS year
),

base_fees AS (
    SELECT * FROM {{ ref('int_dynamic_professional_fees') }}
),

inflated_fees AS (
    SELECT
        bf.property_id,
        bf.property_name,
        bf.company_id,
        bf.portfolio_id,
        y.year,
        bf.unit_count,
        bf.gross_annual_income,
        bf.investment_strategy,

        -- INFLATED ANNUAL FEES BY COMPONENT
        ROUND((bf.base_property_management_fee * POWER(1 + bf.property_mgmt_inflation_rate, y.year - 1))::numeric, 2) AS annual_property_management_fee,
        ROUND((bf.base_asset_management_fee * POWER(1 + bf.asset_mgmt_inflation_rate, y.year - 1))::numeric, 2) AS annual_asset_management_fee,
        ROUND((bf.base_leasing_fee * POWER(1 + bf.leasing_inflation_rate, y.year - 1))::numeric, 2) AS annual_leasing_fee,
        ROUND((bf.base_maintenance_coordination_fee * POWER(1 + bf.maintenance_inflation_rate, y.year - 1))::numeric, 2) AS annual_maintenance_coordination_fee,

        -- CALCULATED MONTHLY FEES
        ROUND((bf.base_property_management_fee * POWER(1 + bf.property_mgmt_inflation_rate, y.year - 1) / 12)::numeric, 2) AS monthly_property_management_fee,
        ROUND((bf.base_asset_management_fee * POWER(1 + bf.asset_mgmt_inflation_rate, y.year - 1) / 12)::numeric, 2) AS monthly_asset_management_fee,
        ROUND((bf.base_leasing_fee * POWER(1 + bf.leasing_inflation_rate, y.year - 1) / 12)::numeric, 2) AS monthly_leasing_fee,
        ROUND((bf.base_maintenance_coordination_fee * POWER(1 + bf.maintenance_inflation_rate, y.year - 1) / 12)::numeric, 2) AS monthly_maintenance_coordination_fee,

        -- CONFIGURATION REFERENCE
        bf.property_mgmt_base_pct,
        bf.asset_mgmt_base_pct,
        bf.leasing_base_pct,
        bf.maintenance_base_pct,
        bf.property_mgmt_inflation_rate,
        bf.asset_mgmt_inflation_rate,
        bf.leasing_inflation_rate,
        bf.maintenance_inflation_rate,
        bf.fee_category

    FROM base_fees bf
    CROSS JOIN years y
)

SELECT
    property_id,
    company_id,
    portfolio_id,
    year,
    property_name,
    unit_count,
    gross_annual_income,
    investment_strategy,

    -- ANNUAL PROFESSIONAL FEES (for OpEx integration)
    annual_property_management_fee,
    annual_asset_management_fee,
    annual_leasing_fee,
    annual_maintenance_coordination_fee,

    -- CALCULATED MONTHLY FEES
    monthly_property_management_fee,
    monthly_asset_management_fee,
    monthly_leasing_fee,
    monthly_maintenance_coordination_fee,

    -- TOTAL ANNUAL PROFESSIONAL FEES (for OpEx calculation)
    ROUND((
        annual_property_management_fee +
        annual_asset_management_fee +
        annual_leasing_fee +
        annual_maintenance_coordination_fee
    )::numeric, 2) AS total_annual_professional_fees,

    -- PER-UNIT CALCULATIONS
    ROUND((annual_property_management_fee / NULLIF(unit_count, 0))::numeric, 2) AS property_mgmt_fee_per_unit,
    ROUND((annual_asset_management_fee / NULLIF(unit_count, 0))::numeric, 2) AS asset_mgmt_fee_per_unit,
    ROUND(((annual_property_management_fee + annual_asset_management_fee + 
            annual_leasing_fee + annual_maintenance_coordination_fee) / 
           NULLIF(unit_count, 0))::numeric, 2) AS total_fee_per_unit,

    -- AS PERCENTAGE OF ORIGINAL PGI
    ROUND((annual_property_management_fee / NULLIF(gross_annual_income, 0) * 100)::numeric, 2) AS property_mgmt_pct_of_pgi,
    ROUND((annual_asset_management_fee / NULLIF(gross_annual_income, 0) * 100)::numeric, 2) AS asset_mgmt_pct_of_pgi,
    ROUND((annual_leasing_fee / NULLIF(gross_annual_income, 0) * 100)::numeric, 2) AS leasing_pct_of_pgi,
    ROUND((annual_maintenance_coordination_fee / NULLIF(gross_annual_income, 0) * 100)::numeric, 2) AS maintenance_pct_of_pgi,
    ROUND(((annual_property_management_fee + annual_asset_management_fee + 
            annual_leasing_fee + annual_maintenance_coordination_fee) / 
           NULLIF(gross_annual_income, 0) * 100)::numeric, 2) AS total_professional_fees_pct_of_pgi,

    -- CONFIGURATION REFERENCE (for transparency)
    property_mgmt_base_pct,
    asset_mgmt_base_pct,
    leasing_base_pct,
    maintenance_base_pct,
    property_mgmt_inflation_rate,
    asset_mgmt_inflation_rate,
    leasing_inflation_rate,
    maintenance_inflation_rate,
    fee_category,

    -- METADATA
    CURRENT_TIMESTAMP AS calculated_at,
    'int_fee_calculations_by_year_dynamic' AS model_source

FROM inflated_fees
WHERE company_id = 1  -- Company scoping
ORDER BY property_id, year