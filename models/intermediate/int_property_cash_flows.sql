{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['property_id'], 'unique': false},
      {'columns': ['portfolio_id'], 'unique': false},
      {'columns': ['year'], 'unique': false}
    ]
  )
}}

-- SOPHISTICATED MULTI-YEAR CASH FLOW MODEL
-- Preserves all original business logic: PGI growth, workforce housing protection, CapEx float income
-- References only staging and intermediate models for clean data lineage

WITH pgi_calc AS (
    WITH RECURSIVE pgi_recursive AS (
        -- Base case: Year 1 for each property
        SELECT 
            pi.property_id,
            pi.company_id,
            pi.portfolio_id,
            1 as year,
            pi.unit_count,
            pi.avg_rent_per_unit,
            pi.init_turn_rate,
            pi.norm_turn_rate,
            pi.cola_snap,
            pi.norm_snap,
            pi.reno_snap,
            pi.vacancy_rate,
            pi.collections_loss_rate,
            pi.opex_ratio,
            pi.capex_per_unit,
            pi.ds_refi_year,
            -- Year 1 PGI: Initial stabilization with workforce housing protection
            ROUND(
                (pi.unit_count * pi.avg_rent_per_unit * 12 * (
                    -- Majority retain with COLA protection (no displacement)
                    (1 - pi.init_turn_rate) * (1 + pi.cola_snap) +
                    -- Small % natural turnover: renovate + market snap with vacancy
                    pi.init_turn_rate * (1 + pi.reno_snap) * (10.0/12)
                ))::numeric, 2
            ) AS pgi
        FROM hkh_dev.stg_property_inputs pi
        WHERE pi.company_id = 1  -- Company scoping for future multi-tenancy
        
        UNION ALL
        
        -- Recursive case: Years 2-20 based on previous year
        SELECT 
            pr.property_id,
            pr.company_id,
            pr.portfolio_id,
            pr.year + 1,
            pr.unit_count,
            pr.avg_rent_per_unit,
            pr.init_turn_rate,
            pr.norm_turn_rate,
            pr.cola_snap,
            pr.norm_snap,
            pr.reno_snap,
            pr.vacancy_rate,
            pr.collections_loss_rate,
            pr.opex_ratio,
            pr.capex_per_unit,
            pr.ds_refi_year,
            -- Years 2+: Stable operations with protected tenants
            ROUND(
                (pr.pgi * (
                    -- Most tenants stay with COLA-only increases (workforce housing protection)
                    (1 - pr.norm_turn_rate) * (1 + pr.cola_snap) +
                    -- Small % natural turnover gets market adjustment
                    pr.norm_turn_rate * (1 + pr.norm_snap)
                ))::numeric, 2
            ) AS pgi
        FROM pgi_recursive pr
        WHERE pr.year < 20
    )
    
    SELECT 
        property_id,
        company_id,
        portfolio_id,
        year,
        pgi,
        vacancy_rate,
        collections_loss_rate,
        opex_ratio,
        unit_count,
        capex_per_unit,
        ds_refi_year
    FROM pgi_recursive
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
        ROUND((pc.pgi * COALESCE(pc.vacancy_rate, 0))::numeric, 0) AS vacancy_loss,
        
        -- Step 2: Calculate gross collectible rent (PGI - vacancy)
        ROUND((pc.pgi - (pc.pgi * COALESCE(pc.vacancy_rate, 0)))::numeric, 0) AS gross_collectible_rent
    FROM pgi_calc pc
),

-- Calculate collections and EGI based on clean intermediate values
collections_calcs AS (
    SELECT
        rc.*,
        
        -- Step 3: Calculate collections loss on collectible rent
        ROUND((rc.gross_collectible_rent * COALESCE(rc.collections_loss_rate, 0))::numeric, 0) AS collections_loss,
        
        -- Step 4: Calculate EGI (Effective Gross Income)
        ROUND((rc.gross_collectible_rent - (rc.gross_collectible_rent * COALESCE(rc.collections_loss_rate, 0)))::numeric, 0) AS egi
    FROM revenue_calcs rc
),

-- Calculate operating expenses (BASE only, no management fees)
operating_calcs AS (
    SELECT
        cc.*,
        
        -- Step 5: Calculate BASE operating expenses only
        ROUND((cc.egi * COALESCE(cc.opex_ratio, 0.30))::numeric, 0) AS opex,
        
        -- Step 6: Calculate NOI (Net Operating Income)
        ROUND((cc.egi - (cc.egi * COALESCE(cc.opex_ratio, 0.30)))::numeric, 0) AS noi
    FROM collections_calcs cc
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
        ROUND((oc.noi - COALESCE(lp.debt_service, 0))::numeric, 0) AS btcf,
        
        -- Step 8: BTCF minus CapEx spending (before float income)
        ROUND((oc.noi - COALESCE(lp.debt_service, 0) - COALESCE(cr.capex, 0))::numeric, 0) AS btcf_after_capex,
        
        -- Step 9: Final ATCF Operations (after adding float income) - THE CRITICAL INTEGRATION
        ROUND(
            (oc.noi - COALESCE(lp.debt_service, 0) - COALESCE(cr.capex, 0) + COALESCE(cr.capex_float_income, 0))::numeric, 0
        ) AS atcf_operations,
        
        -- Annual cash flow metrics for business analysis
        ROUND((oc.egi * (1 - COALESCE(oc.opex_ratio, 0.30)))::numeric, 0) AS annual_noi,
        ROUND((oc.noi - COALESCE(lp.debt_service, 0) - COALESCE(cr.capex, 0) + COALESCE(cr.capex_float_income, 0))::numeric, 0) AS annual_cash_flow_after_capex
        
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