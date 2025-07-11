{{
  config(
    materialized='view'
  )
}}

-- SOPHISTICATED MULTI-YEAR CASH FLOW MODEL
-- Preserves all original business logic: PGI growth, workforce housing protection, CapEx float income
-- References only staging and intermediate models for clean data lineage

WITH 
-- Reference extracted PGI model
pgi_data AS (
    SELECT * FROM {{ ref('int_pgi_calculations') }}
),

-- Get loan payments from intermediate model
loan_payments AS (
    SELECT 
        property_id,
        year,
        COALESCE(annual_payment, 0) AS debt_service,
        COALESCE(refi_proceeds, 0) as refi_proceeds
    FROM hkh_dev.int_loan_schedules
),

-- Get capex reserves from intermediate model
capex_reserves AS (
    SELECT 
        property_id,
        year,
        interest_income AS capex_float_income,
        capex_spent AS capex,
        ending_reserve_balance AS reserve_balance
    FROM hkh_dev.int_capex_reserves
),

-- Get professional fees from NEW fees mart (replaces old multi-model approach)
professional_fees AS (
    SELECT
        property_id,
        year,
        SUM(fee_amount) as total_annual_professional_fees
    FROM {{ ref('fact_opex_fees_calculations') }}
    GROUP BY property_id, year
),

-- NEW: Get sophisticated property expenses from detailed fact mart
property_expenses AS (
    SELECT
        property_id,
        year,
        SUM(expense_amount) as total_property_expenses
    FROM {{ ref('fact_opex_expenses_calculations') }}
    GROUP BY property_id, year
),

-- Clean, step-by-step revenue calculations
revenue_calcs AS (
    SELECT
        pc.property_id,
        pc.company_id,
        pc.portfolio_id,
        pc.year,
        pc.pgi,
        pc.vacancy_rate,
        pc.collections_loss_rate,
        pc.opex_ratio,
        
        -- Step 1: Calculate vacancy loss
        ROUND(pc.pgi * COALESCE(pc.vacancy_rate, 0), 0) AS vacancy_loss,
        
        -- Step 2: Calculate gross collectible rent (PGI - vacancy)
        ROUND(pc.pgi - (pc.pgi * COALESCE(pc.vacancy_rate, 0)), 0) AS gross_collectible_rent
    FROM pgi_data pc
),

-- Calculate collections and EGI based on clean intermediate values
collections_calcs AS (
    SELECT
        rc.*,
        
        -- Step 3: Calculate collections loss on collectible rent
        ROUND(rc.gross_collectible_rent * COALESCE(rc.collections_loss_rate, 0), 0) AS collections_loss,
        
        -- Step 4: Calculate EGI (Effective Gross Income)
        ROUND(rc.gross_collectible_rent - (rc.gross_collectible_rent * COALESCE(rc.collections_loss_rate, 0)), 0) AS egi
    FROM revenue_calcs rc
),

-- Calculate operating expenses (sophisticated fees + sophisticated property expenses)
operating_calcs AS (
    SELECT
        cc.*,
        ROUND(COALESCE(pf.total_annual_professional_fees, 0), 0) AS professional_fees,
        
        -- NEW: Sophisticated base property OpEx (replaces 10.5% plug)
        ROUND(COALESCE(pe.total_property_expenses, 0), 0) AS base_property_opex,
        
        -- Total sophisticated OpEx (professional fees + sophisticated property expenses)
        ROUND(COALESCE(pf.total_annual_professional_fees, 0) + COALESCE(pe.total_property_expenses, 0), 0) AS opex,
        
        -- NOI with sophisticated OpEx
        ROUND(cc.egi - (COALESCE(pf.total_annual_professional_fees, 0) + COALESCE(pe.total_property_expenses, 0)), 0) AS noi
    FROM collections_calcs cc
    LEFT JOIN professional_fees pf ON cc.property_id = pf.property_id AND cc.year = pf.year
    LEFT JOIN property_expenses pe ON cc.property_id = pe.property_id AND cc.year = pe.year
),

-- Calculate sophisticated cash flows WITH CapEx float income integration
cash_flow_calcs AS (
    SELECT
        oc.*,
        COALESCE(lp.debt_service, 0) as debt_service,
        COALESCE(cr.capex, 0) as capex,
        COALESCE(cr.reserve_balance, 0) as reserve_balance,
        COALESCE(cr.capex_float_income, 0) as capex_float_income,
        COALESCE(lp.refi_proceeds, 0) as refi_proceeds,
        
        -- Step 7: Calculate BTCF (Before-Tax Cash Flow)
        ROUND(oc.noi - COALESCE(lp.debt_service, 0), 0) AS btcf,
        
        -- Step 8: BTCF minus CapEx spending (before float income)
        ROUND(oc.noi - COALESCE(lp.debt_service, 0) - COALESCE(cr.capex, 0), 0) AS btcf_after_capex,
        
        -- Step 9: Final ATCF Operations (after adding float income) - THE CRITICAL INTEGRATION
        ROUND(oc.noi - COALESCE(lp.debt_service, 0) - COALESCE(cr.capex, 0) + COALESCE(cr.capex_float_income, 0), 0) AS atcf_operations,
        
        -- Annual cash flow metrics for business analysis
        ROUND(oc.noi, 0) AS annual_noi,
        ROUND(oc.noi - COALESCE(lp.debt_service, 0) - COALESCE(cr.capex, 0) + COALESCE(cr.capex_float_income, 0), 0) AS annual_cash_flow_after_capex
        
    FROM operating_calcs oc
    LEFT JOIN loan_payments lp ON lp.property_id = oc.property_id AND lp.year = oc.year
    LEFT JOIN capex_reserves cr ON cr.property_id = oc.property_id AND cr.year = oc.year
)

-- FINAL SELECT: Sophisticated multi-year cash flows with all business logic preserved
SELECT
    property_id,
    company_id,
    portfolio_id,
    year,
    
    -- Income progression
    pgi,
    vacancy_loss,
    collections_loss,
    egi,
    
    -- Sophisticated OpEx breakdown
    professional_fees,
    base_property_opex,
    opex,
    noi,
    annual_noi,
    
    -- Financing & CapEx
    debt_service,
    capex,
    capex_float_income,          -- CRITICAL: Interest earnings from reserves
    reserve_balance,
    
    -- Cash flows
    btcf,                        -- Before-Tax Cash Flow
    btcf_after_capex,           -- BTCF after CapEx spending
    atcf_operations,            -- Final After-Tax Cash Flow Operations (includes float income)
    annual_cash_flow_after_capex, -- Annual summary metric
    refi_proceeds,              -- Refinancing proceeds
    
    -- Metadata
    CURRENT_TIMESTAMP AS calculated_at,
    'int_property_cash_flows' AS model_source
    
FROM cash_flow_calcs
WHERE company_id = 1
ORDER BY property_id, year 