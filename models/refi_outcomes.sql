-- models/fact_property_cash_flow.sql
-- FIXED VERSION: Uses working loan schedule instead of broken refi_outcomes

WITH assumptions AS (
    SELECT
        property_id,
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
        purchase_price,
        ds_ltv,
        ds_int,
        ds_term,
        capex_per_unit,
        ds_refi_year
    FROM {{ source('inputs', 'property_inputs') }}
),

-- FIXED PGI CALCULATION: Now actually uses your workforce housing variables
pgi_calc AS (
    WITH RECURSIVE pgi_recursive AS (
        -- Year 1: Initial stabilization period with incentive to sign long-term leases
        SELECT 
            a.property_id,
            1 as year,
            ROUND(
                a.unit_count * a.avg_rent_per_unit * 12 * (
                    -- Majority retain and sign long-term leases with COLA protection
                    (1 - a.init_turn_rate) * (1 + a.cola_snap) +
                    -- Small % natural turnover: renovate + market snap but with 2-month vacancy
                    a.init_turn_rate * (1 + a.reno_snap) * (10.0/12)
                ), 0
            ) AS pgi,
            a.vacancy_rate,
            a.collections_loss_rate,
            a.opex_ratio,
            a.unit_count,
            a.capex_per_unit,
            a.ds_refi_year,
            a.norm_turn_rate,
            a.cola_snap,
            a.norm_snap
        FROM {{ source('inputs', 'property_inputs') }} a
        
        UNION ALL
        
        -- Years 2+: Stable operations with protected tenants (NO DISPLACEMENT)
        SELECT 
            pr.property_id,
            pr.year + 1,
            ROUND(
                pr.pgi * (
                    -- Most tenants stay with COLA-only increases
                    (1 - pr.norm_turn_rate) * (1 + pr.cola_snap) +
                    -- Small % natural turnover gets market adjustment when they voluntarily leave
                    pr.norm_turn_rate * (1 + pr.norm_snap)
                ), 2
            ) AS pgi,
            pr.vacancy_rate,
            pr.collections_loss_rate,
            pr.opex_ratio,
            pr.unit_count,
            pr.capex_per_unit,
            pr.ds_refi_year,
            pr.norm_turn_rate,
            pr.cola_snap,
            pr.norm_snap
        FROM pgi_recursive pr
        WHERE pr.year < 20
    )
    
    SELECT 
        property_id,
        year,
        pgi::INTEGER AS pgi,
        vacancy_rate,
        collections_loss_rate,
        opex_ratio,
        unit_count,
        capex_per_unit,
        ds_refi_year
    FROM pgi_recursive
),

-- FIXED: Use your working loan amortization schedule instead of broken refi logic
loan_payments AS (
    SELECT 
        property_id,
        year,
        annual_payment AS debt_service,
        refi_proceeds
    FROM {{ ref('loan_amort_schedule') }}
),

capex_calc AS (
    SELECT
        f.property_id,
        f.year,
        ROUND(rra.unit_count * rra.capex_per_unit * f.capex_factor, 0) AS capex
    FROM {{ source('inputs', 'capex_factors') }} f
    JOIN {{ source('inputs', 'property_inputs') }} rra ON rra.property_id = f.property_id
)

SELECT
    'micro-1' AS portfolio_id,
    pc.property_id,
    pc.year,
    pc.pgi,
    ROUND(pc.pgi * COALESCE(pc.vacancy_rate, 0), 0) AS vacancy_loss,
    ROUND((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0)) * COALESCE(pc.collections_loss_rate, 0), 0) AS collections_loss,
    ROUND(
        pc.pgi
        - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
        - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0),
        0
    ) AS egi,
    ROUND(
        (
            pc.pgi
            - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
            - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0)
        ) * COALESCE(pc.opex_ratio, 0.30),
        0
    ) AS opex,
    ROUND(
        (
            pc.pgi
            - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
            - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0)
        ) - (
            (
                pc.pgi
                - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
                - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0)
            ) * COALESCE(pc.opex_ratio, 0.30)
        ),
        0
    ) AS noi,
    lp.debt_service,
    ROUND(
        (
            (
                pc.pgi
                - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
                - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0)
            ) - (
                (
                    pc.pgi
                    - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
                    - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0)
                ) * COALESCE(pc.opex_ratio, 0.30)
            )
        ) - lp.debt_service,
        0
    ) AS btcf,
    cx.capex,
    ROUND(
        (
            (
                (
                    pc.pgi
                    - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
                    - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0)
                ) - (
                    (
                        pc.pgi
                        - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
                        - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0)
                    ) * COALESCE(pc.opex_ratio, 0.30)
                )
            ) - lp.debt_service
        ) - COALESCE(cx.capex, 0)
        + COALESCE(lp.refi_proceeds, 0),
        0
    ) AS atcf

FROM pgi_calc pc
LEFT JOIN assumptions pi ON pi.property_id = pc.property_id
LEFT JOIN loan_payments lp ON lp.property_id = pc.property_id AND lp.year = pc.year
LEFT JOIN capex_calc cx ON cx.property_id = pc.property_id AND cx.year = pc.year