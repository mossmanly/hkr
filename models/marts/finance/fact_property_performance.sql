-- marts/finance/fact_property_performance.sql
-- Property performance tracking with year-over-year analysis
-- FIXED: Uses actual column names from complete schema analysis

{{ config(materialized='table') }}

WITH property_cash_flows AS (
    SELECT 
        pcf.property_id,
        pcf.company_id,
        pcf.portfolio_id,
        pcf.year,
        
        -- Cash flow data from int_property_cash_flows (actual columns)
        ROUND(pcf.pgi, 0) AS pgi,
        ROUND(pcf.vacancy_loss, 0) AS vacancy_loss,
        ROUND(pcf.collections_loss, 0) AS collections_loss,
        ROUND(pcf.egi, 0) AS egi,
        ROUND(pcf.opex, 0) AS opex,
        ROUND(pcf.noi, 0) AS noi,
        ROUND(pcf.annual_noi, 0) AS annual_noi,
        ROUND(pcf.debt_service, 0) AS debt_service,
        ROUND(pcf.capex, 0) AS capex,
        ROUND(pcf.capex_float_income, 0) AS capex_float_income,
        ROUND(pcf.reserve_balance, 0) AS reserve_balance,
        ROUND(pcf.btcf, 0) AS btcf,
        ROUND(pcf.btcf_after_capex, 0) AS btcf_after_capex,
        ROUND(pcf.atcf_operations, 0) AS atcf_operations,
        ROUND(pcf.annual_cash_flow_after_capex, 0) AS annual_cash_flow_after_capex,
        ROUND(pcf.refi_proceeds, 0) AS refi_proceeds

    FROM {{ ref('int_property_cash_flows') }} pcf
    WHERE pcf.company_id = 1
),

property_basics AS (
    SELECT 
        fc.property_id,
        fc.company_id,
        fc.portfolio_id,
        fc.property_name,
        ROUND(fc.purchase_price, 0) AS purchase_price,
        fc.unit_count,
        ROUND(fc.gross_annual_income, 0) AS gross_annual_income,
        fc.portfolio_name,
        fc.investment_strategy,
        
        -- Property identification from staging
        spi.property_address,
        spi.city,
        spi.zip,
        spi.building_class,
        spi.acquisition_year,
        ROUND(spi.avg_rent_per_unit, 0) AS avg_rent_per_unit

    FROM {{ ref('int_fee_calculations') }} fc
    LEFT JOIN hkh_dev.stg_property_inputs spi
        ON fc.property_id = spi.property_id
        AND fc.company_id = spi.company_id
    WHERE fc.company_id = 1
),

fee_data AS (
    SELECT 
        fc.property_id,
        fc.company_id,
        fc.portfolio_id,
        ROUND(fc.acquisition_fee, 0) AS acquisition_fee,
        ROUND(fc.annual_management_fee, 0) AS annual_management_fee,
        ROUND(fc.monthly_management_fee, 0) AS monthly_management_fee,
        ROUND(fc.estimated_disposition_fee, 0) AS estimated_disposition_fee,
        ROUND(fc.total_annual_fees, 0) AS total_annual_fees,
        fc.management_fee_rate,
        ROUND(fc.management_fee_per_unit, 0) AS management_fee_per_unit,
        fc.fee_category

    FROM {{ ref('int_fee_calculations') }} fc
    WHERE fc.company_id = 1
)

SELECT
    -- Primary Keys
    pcf.property_id,
    pcf.company_id,
    pcf.portfolio_id,
    pcf.year,
    
    -- Property Information
    pb.property_name,
    pb.property_address,
    pb.city,
    pb.zip,
    pb.building_class,
    pb.acquisition_year,
    pb.portfolio_name,
    pb.investment_strategy,
    
    -- Property Fundamentals (rounded to whole dollars)
    pb.purchase_price,
    pb.unit_count,
    pb.avg_rent_per_unit,
    pb.gross_annual_income,
    
    -- Annual Cash Flow Performance
    pcf.pgi,
    pcf.vacancy_loss,
    pcf.collections_loss,
    pcf.egi,
    pcf.opex,
    pcf.noi,
    pcf.annual_noi,
    pcf.debt_service,
    pcf.capex,
    pcf.capex_float_income,
    pcf.reserve_balance,
    pcf.btcf,
    pcf.btcf_after_capex,
    pcf.atcf_operations,
    pcf.annual_cash_flow_after_capex,
    pcf.refi_proceeds,
    
    -- Fee Structure
    COALESCE(fd.acquisition_fee, 0) AS acquisition_fee,
    COALESCE(fd.annual_management_fee, 0) AS annual_management_fee,
    COALESCE(fd.monthly_management_fee, 0) AS monthly_management_fee,
    COALESCE(fd.estimated_disposition_fee, 0) AS estimated_disposition_fee,
    COALESCE(fd.total_annual_fees, 0) AS total_annual_fees,
    COALESCE(fd.management_fee_rate, 0) AS management_fee_rate,
    COALESCE(fd.management_fee_per_unit, 0) AS management_fee_per_unit,
    COALESCE(fd.fee_category, 'Standard') AS fee_category,
    
    -- Performance Calculations
    ROUND(pcf.noi / NULLIF(pb.purchase_price, 0) * 100, 2) AS cap_rate,
    ROUND(pcf.annual_cash_flow_after_capex / NULLIF(pb.purchase_price - (pb.purchase_price * 0.75), 0) * 100, 2) AS estimated_cash_on_cash_return,
    ROUND(pcf.noi / NULLIF(pcf.debt_service, 0), 2) AS debt_service_coverage_ratio,
    ROUND((pcf.noi - COALESCE(fd.total_annual_fees, 0)) / NULLIF(pb.purchase_price, 0) * 100, 2) AS net_yield_after_fees,
    
    -- Unit Economics
    ROUND(pcf.noi / pb.unit_count, 0) AS noi_per_unit,
    ROUND(pcf.annual_cash_flow_after_capex / pb.unit_count, 0) AS cash_flow_per_unit,
    ROUND(pcf.capex / pb.unit_count, 0) AS capex_per_unit,
    
    -- Performance Categories
    CASE 
        WHEN pcf.year = 1 THEN 'Year 1 Projections'
        WHEN pcf.year <= 3 THEN 'Early Years (2-3)'
        WHEN pcf.year <= 7 THEN 'Mid-Term (4-7)'
        ELSE 'Long-Term (8+)'
    END AS performance_period,
    
    CASE 
        WHEN ROUND(pcf.noi / NULLIF(pb.purchase_price, 0) * 100, 2) >= 8 THEN 'High Cap Rate (8%+)'
        WHEN ROUND(pcf.noi / NULLIF(pb.purchase_price, 0) * 100, 2) >= 6 THEN 'Medium Cap Rate (6-8%)'
        ELSE 'Low Cap Rate (<6%)'
    END AS cap_rate_category,
    
    CASE 
        WHEN ROUND(pcf.noi / NULLIF(pcf.debt_service, 0), 2) >= 1.4 THEN 'Strong Coverage (1.4+)'
        WHEN ROUND(pcf.noi / NULLIF(pcf.debt_service, 0), 2) >= 1.2 THEN 'Adequate Coverage (1.2-1.4)'
        ELSE 'Tight Coverage (<1.2)'
    END AS coverage_category,
    
    -- Year-over-Year Growth Calculations (for years > 1)
    CASE 
        WHEN pcf.year > 1 THEN 
            LAG(pcf.noi) OVER (PARTITION BY pcf.property_id ORDER BY pcf.year)
        ELSE NULL 
    END AS prior_year_noi,
    
    CASE 
        WHEN pcf.year > 1 THEN 
            ROUND((pcf.noi - LAG(pcf.noi) OVER (PARTITION BY pcf.property_id ORDER BY pcf.year)) / 
                  NULLIF(LAG(pcf.noi) OVER (PARTITION BY pcf.property_id ORDER BY pcf.year), 0) * 100, 2)
        ELSE NULL 
    END AS noi_growth_percent,
    
    CASE 
        WHEN pcf.year > 1 THEN 
            ROUND((pcf.annual_cash_flow_after_capex - LAG(pcf.annual_cash_flow_after_capex) OVER (PARTITION BY pcf.property_id ORDER BY pcf.year)) / 
                  NULLIF(LAG(pcf.annual_cash_flow_after_capex) OVER (PARTITION BY pcf.property_id ORDER BY pcf.year), 0) * 100, 2)
        ELSE NULL 
    END AS cash_flow_growth_percent

FROM property_cash_flows pcf
LEFT JOIN property_basics pb 
    ON pcf.property_id = pb.property_id 
    AND pcf.company_id = pb.company_id 
    AND pcf.portfolio_id = pb.portfolio_id
LEFT JOIN fee_data fd 
    ON pcf.property_id = fd.property_id 
    AND pcf.company_id = fd.company_id 
    AND pcf.portfolio_id = fd.portfolio_id

ORDER BY pcf.property_id, pcf.year