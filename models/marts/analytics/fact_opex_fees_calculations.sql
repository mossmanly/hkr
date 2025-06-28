{{ config(materialized='view') }}

-- Comprehensive OPEX fees mart for business analysis and cash flow reconciliation

WITH years AS (
    SELECT generate_series(1, 20) AS year
),

property_data AS (
    SELECT 
        property_id,
        property_name,
        unit_count,
        purchase_price,
        portfolio_id,
        company_id
    FROM hkh_dev.stg_property_inputs
),

pgi_data AS (
    SELECT 
        property_id,
        year,
        pgi
    FROM {{ ref('int_pgi_calculations') }}
),

fees_config AS (
    SELECT 
        company_id,
        portfolio_id,
        fee_component as category,
        base_pct_of_pgi,
        annual_inflation_rate
    FROM {{ ref('stg_opex_fees_inputs') }}
),

base_calculations AS (
    SELECT 
        p.property_id,
        p.property_name,
        p.unit_count,
        p.purchase_price,
        p.company_id,
        p.portfolio_id,
        f.category,
        f.base_pct_of_pgi,
        f.annual_inflation_rate,
        y.year,
        pgi.pgi,
        
        -- Calculate base fee (year 1) - rounded to whole dollars
        ROUND(pgi.pgi * f.base_pct_of_pgi / 100.0, 0) AS base_fee_amount,
        
        -- Calculate inflated fee for this year - rounded to whole dollars
        ROUND(pgi.pgi * f.base_pct_of_pgi / 100.0 * 
              POWER(1 + f.annual_inflation_rate / 100.0, y.year - 1), 0) AS fee_amount
        
    FROM property_data p
    CROSS JOIN years y
    CROSS JOIN fees_config f
    LEFT JOIN pgi_data pgi ON p.property_id = pgi.property_id AND y.year = pgi.year
    WHERE f.company_id = p.company_id 
      AND f.portfolio_id = p.portfolio_id
)

SELECT 
    property_id,
    property_name,
    unit_count,
    purchase_price,
    category,
    year,
    pgi,
    base_pct_of_pgi,
    annual_inflation_rate,
    base_fee_amount,
    fee_amount,
    
    -- Calculate actual % of PGI for this inflated year
    CASE 
        WHEN pgi > 0 THEN (fee_amount / pgi) * 100.0 
        ELSE 0 
    END AS fee_pct_of_pgi,
    
    -- Add company_id and portfolio_id for downstream models
    company_id,
    portfolio_id
    
FROM base_calculations
WHERE pgi IS NOT NULL  -- Only include years with PGI data
ORDER BY property_id, category, year 