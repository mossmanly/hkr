-- models/fact_property_cash_flow_base.sql
-- BASE MODEL: Cash flows WITHOUT management fees (breaks circular dependency)

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
                unit_count * avg_rent_per_unit * 12 * (
                    -- Majority retain with COLA protection (no displacement)
                    (1 - init_turn_rate) * (1 + cola_snap) +
                    -- Small % natural turnover: renovate + market snap with vacancy
                    init_turn_rate * (1 + reno_snap) * (10.0/12)
                ), 2
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
                pr.pgi * (
                    -- Most tenants stay with COLA-only increases (workforce housing protection)
                    (1 - pr.norm_turn_rate) * (1 + pr.cola_snap) +
                    -- Small % natural turnover gets market adjustment
                    pr.norm_turn_rate * (1 + pr.norm_snap)
                ), 2
            ) AS pgi
        FROM pgi_recursive pr  -- FIXED: Only reference recursive CTE, not source table
        WHERE pr.year < 20     -- Stop at year 20
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
        annual_payment AS debt_service,
        refi_proceeds
    FROM {{ ref('loan_amort_schedule') }}
),

-- Use your existing capex calculation
capex_calc AS (
    SELECT
        f.property_id,
        f.year,
        ROUND(rra.unit_count * rra.capex_per_unit * f.capex_factor, 0) AS capex
    FROM {{ source('inputs', 'capex_factors') }} f
    JOIN {{ source('inputs', 'property_inputs') }} rra ON rra.property_id = f.property_id
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
        ROUND(pc.pgi * COALESCE(pc.vacancy_rate, 0), 0) AS vacancy_loss,
        
        -- Step 2: Calculate gross collectible rent (PGI - vacancy)
        ROUND(pc.pgi - (pc.pgi * COALESCE(pc.vacancy_rate, 0)), 0) AS gross_collectible_rent
        
    FROM pgi_calc pc
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

-- Calculate operating expenses (BASE only, no management fees)
operating_calcs AS (
    SELECT
        cc.*,
        
        -- Step 5: Calculate BASE operating expenses only
        ROUND(cc.egi * COALESCE(cc.opex_ratio, 0.30), 0) AS opex,
        
        -- Step 6: Calculate NOI (Net Operating Income)
        ROUND(cc.egi - (cc.egi * COALESCE(cc.opex_ratio, 0.30)), 0) AS noi
        
    FROM collections_calcs cc
),

-- Calculate cash flows (BASE only, no management fees)
cash_flow_calcs AS (
    SELECT
        oc.*,
        lp.debt_service,
        cx.capex,
        
        -- Step 7: Calculate BTCF (Before-Tax Cash Flow)
        ROUND(oc.noi - COALESCE(lp.debt_service, 0), 0) AS btcf,
        
        -- Step 8: Calculate ATCF Operations (After-Tax Cash Flow from Operations)
        ROUND(oc.noi - COALESCE(lp.debt_service, 0) - COALESCE(cx.capex, 0), 0) AS atcf_operations,
        
        -- Step 9: Refi proceeds (when applicable)
        ROUND(COALESCE(lp.refi_proceeds, 0), 0) AS atcf_refi
        
    FROM operating_calcs oc
    LEFT JOIN loan_payments lp ON lp.property_id = oc.property_id AND lp.year = oc.year
    LEFT JOIN capex_calc cx ON cx.property_id = oc.property_id AND cx.year = oc.year
)

-- FINAL SELECT: BASE cash flows (no management fees)
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
    btcf,
    capex,
    atcf_operations,
    atcf_refi
FROM cash_flow_calcs
ORDER BY property_id, year