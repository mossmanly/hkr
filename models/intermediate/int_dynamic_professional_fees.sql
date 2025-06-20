{{ config(materialized='table') }}

-- Dynamic professional fee calculation from configuration percentages
-- Replaces hard-coded base fee amounts with config-driven approach

WITH property_base AS (
    SELECT
        property_id,
        property_name,
        company_id,
        portfolio_id,
        unit_count,
        gross_annual_income,
        investment_strategy,
        purchase_price
    FROM {{ source('inputs', 'stg_base_fees_components') }}
),

fee_config AS (
    SELECT
        company_id,
        portfolio_id,
        fee_component,
        base_pct_of_pgi,
        annual_inflation_rate,
        notes,
        market_benchmark_source
    FROM {{ ref('stg_fees_configuration') }}
),

fee_calculations AS (
    SELECT
        p.property_id,
        p.property_name,
        p.company_id,
        p.portfolio_id,
        p.unit_count,
        p.gross_annual_income,
        p.investment_strategy,

        -- DYNAMIC BASE FEES (calculated from config percentages)
        ROUND((p.gross_annual_income * MAX(CASE WHEN fc.fee_component = 'property_mgmt' THEN fc.base_pct_of_pgi ELSE 0 END) / 100.0)::numeric, 2) AS base_property_management_fee,
        ROUND((p.gross_annual_income * MAX(CASE WHEN fc.fee_component = 'asset_mgmt' THEN fc.base_pct_of_pgi ELSE 0 END) / 100.0)::numeric, 2) AS base_asset_management_fee,
        ROUND((p.gross_annual_income * MAX(CASE WHEN fc.fee_component = 'leasing' THEN fc.base_pct_of_pgi ELSE 0 END) / 100.0)::numeric, 2) AS base_leasing_fee,
        ROUND((p.gross_annual_income * MAX(CASE WHEN fc.fee_component = 'maintenance' THEN fc.base_pct_of_pgi ELSE 0 END) / 100.0)::numeric, 2) AS base_maintenance_coordination_fee,

        -- CONFIGURATION METADATA
        MAX(CASE WHEN fc.fee_component = 'property_mgmt' THEN fc.base_pct_of_pgi END) AS property_mgmt_base_pct,
        MAX(CASE WHEN fc.fee_component = 'asset_mgmt' THEN fc.base_pct_of_pgi END) AS asset_mgmt_base_pct,
        MAX(CASE WHEN fc.fee_component = 'leasing' THEN fc.base_pct_of_pgi END) AS leasing_base_pct,
        MAX(CASE WHEN fc.fee_component = 'maintenance' THEN fc.base_pct_of_pgi END) AS maintenance_base_pct,

        MAX(CASE WHEN fc.fee_component = 'property_mgmt' THEN fc.annual_inflation_rate END) AS property_mgmt_inflation_rate,
        MAX(CASE WHEN fc.fee_component = 'asset_mgmt' THEN fc.annual_inflation_rate END) AS asset_mgmt_inflation_rate,
        MAX(CASE WHEN fc.fee_component = 'leasing' THEN fc.annual_inflation_rate END) AS leasing_inflation_rate,
        MAX(CASE WHEN fc.fee_component = 'maintenance' THEN fc.annual_inflation_rate END) AS maintenance_inflation_rate

    FROM property_base p
    LEFT JOIN fee_config fc ON p.company_id = fc.company_id
        AND (p.portfolio_id = fc.portfolio_id OR fc.portfolio_id IS NULL)
    GROUP BY 
        p.property_id, p.property_name, p.company_id, p.portfolio_id, 
        p.unit_count, p.gross_annual_income, p.investment_strategy
)

SELECT
    property_id,
    property_name,
    company_id,
    portfolio_id,
    unit_count,
    gross_annual_income,
    investment_strategy,

    -- BASE FEE AMOUNTS (Year 1) - CALCULATED DYNAMICALLY
    base_property_management_fee,
    base_asset_management_fee,
    base_leasing_fee,
    base_maintenance_coordination_fee,

    -- TOTAL BASE FEES
    ROUND((base_property_management_fee + base_asset_management_fee + 
           base_leasing_fee + base_maintenance_coordination_fee)::numeric, 2) AS total_base_professional_fees,

    -- BASE PERCENTAGES (from configuration)
    property_mgmt_base_pct,
    asset_mgmt_base_pct,
    leasing_base_pct,
    maintenance_base_pct,
    ROUND((property_mgmt_base_pct + asset_mgmt_base_pct + leasing_base_pct + 
           maintenance_base_pct)::numeric, 1) AS total_base_pct_of_pgi,

    -- INFLATION RATES (from configuration)
    property_mgmt_inflation_rate,
    asset_mgmt_inflation_rate,
    leasing_inflation_rate,
    maintenance_inflation_rate,

    -- METADATA
    'DYNAMIC_CONFIG' AS fee_category,
    CONCAT('Low-turnover model: ', 
           ROUND((property_mgmt_base_pct + asset_mgmt_base_pct + leasing_base_pct + 
                  maintenance_base_pct)::numeric, 1), 
           '% total professional fees calculated dynamically from configuration') AS fee_basis_notes,
    CURRENT_DATE AS last_calculated_date

FROM fee_calculations 