-- models/fact_property_cash_flow_base.sql
-- ENHANCED MODEL: Cash flows WITH CapEx float income using reserve view

-- Fixed recursive CTE for PGI calculation
WITH pgi_calc AS (
    WITH RECURSIVE pgi_recursive AS (
        -- Base case: Year 1 for each property
        SELECT 
            property_id,
            1 as year,
            unit_count,
            avg_rent_per_unit,
            init_turn_rate,
            norm_turn_rate,
            cola_snap,
            norm_snap,
            reno_snap,
            vacancy_rate,
            collections_loss_rate,
            opex_ratio,
            capex_per_unit,
            ds_refi_year,
            -- Year 1 PGI: Initial stabilization with workforce housing protection
            ROUND(
                (unit_count * avg_rent_per_unit * 12 * (
                    -- Majority retain with COLA protection (no displacement)
                    (1 - init_turn_rate) * (1 + cola_snap) +
                    -- Small % natural turnover: renovate + market snap with vacancy
                    init_turn_rate * (1 + reno_snap) * (10.0/12)
                ))::numeric, 2
            ) AS pgi
        FROM {{ source('inputs', 'property_inputs') }}
        
        UNION ALL
        
        -- Recursive case: Years 2-20 based on previous year
        SELECT 
            pr.property_id,
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

-- Use your existing loan payments
loan_payments AS (
    SELECT 
        property_id,
        year,
        COALESCE(annual_payment, 0) AS debt_service,
        COALESCE(refi_proceeds, 0) as refi_proceeds
    FROM {{ ref('loan_amort_schedule') }}
),

-- CLEAN: Use the reserve management view instead of complex calculations
capex_reserves AS (
    SELECT 
        property_id,
        year,
        interest_income AS capex_float_income,
        capex_spent AS capex,
        ending_reserve_balance AS reserve_balance
    FROM {{ ref('capex_reserve_mgt') }}
),

-- Clean, step-by-step revenue calculations
revenue_calcs AS (
    SELECT
        pc.property_id,
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

-- Calculate cash flows WITH SEPARATE FLOAT INCOME LINE
cash_flow_calcs AS (
    SELECT
        oc.*,
        COALESCE(lp.debt_service, 0) as debt_service,
        COALESCE(cr.capex, 0) as capex,
        COALESCE(cr.reserve_balance, 0) as reserve_balance,
        COALESCE(cr.capex_float_income, 0) as capex_float_income,
        
        -- Step 7: Calculate BTCF (Before-Tax Cash Flow)
        ROUND((oc.noi - COALESCE(lp.debt_service, 0))::numeric, 0) AS btcf,
        
        -- Step 8: BTCF minus CapEx spending (before float income)
        ROUND((oc.noi - COALESCE(lp.debt_service, 0) - COALESCE(cr.capex, 0))::numeric, 0) AS btcf_after_capex,
        
        -- Step 9: Final ATCF Operations (after adding float income)
        ROUND(
            (oc.noi - COALESCE(lp.debt_service, 0) - COALESCE(cr.capex, 0) + COALESCE(cr.capex_float_income, 0))::numeric, 0
        ) AS atcf_operations,
        
        -- Step 10: Refi proceeds (when applicable)
        ROUND(COALESCE(lp.refi_proceeds, 0)::numeric, 0) AS atcf_refi
        
    FROM operating_calcs oc
    LEFT JOIN loan_payments lp ON lp.property_id = oc.property_id AND lp.year = oc.year
    LEFT JOIN capex_reserves cr ON cr.property_id = oc.property_id AND cr.year = oc.year
)

-- FINAL SELECT: Enhanced cash flows WITH SEPARATE FLOAT INCOME LINE
SELECT
    'micro-1' AS portfolio_id,
    property_id,
    year,
    pgi,
    vacancy_loss,
    collections_loss,
    egi,
    opex,
    noi,
    debt_service,
    btcf,                          -- Before-Tax Cash Flow
    capex,                         -- CapEx Spending (negative impact)
    btcf_after_capex,             -- BTCF after CapEx (intermediate step)
    capex_float_income,           -- CapEx Float Income (positive impact) 
    atcf_operations,              -- Final After-Tax Cash Flow Operations
    reserve_balance,              -- Current reserve balance (for monitoring)
    atcf_refi                     -- Refinancing proceeds
FROM cash_flow_calcs
ORDER BY property_id, year