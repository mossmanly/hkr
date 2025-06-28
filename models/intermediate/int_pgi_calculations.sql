{{
  config(
    materialized='view'
  )
}}

-- EXTRACTED PGI CALCULATIONS
-- Enables clean opex modeling by breaking circular dependency
-- EXACT COPY of recursive logic from int_property_cash_flows

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
            pi.unit_count * pi.avg_rent_per_unit * 12 * (
                -- Majority retain with COLA protection (no displacement)
                (1 - pi.init_turn_rate) * (1 + pi.cola_snap) +
                -- Small % natural turnover: renovate + market snap with vacancy
                pi.init_turn_rate * (1 + pi.reno_snap) * (10.0/12)
            ), 0
        ) AS pgi
    FROM {{ source('hkh_dev', 'stg_property_inputs') }} pi
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
        -- 🏆 FIXED: Use reno_snap for turning units (the BIG money!)
        ROUND(
            pr.pgi * (
                -- Most tenants stay with COLA-only increases (workforce housing protection)
                (1 - pr.norm_turn_rate) * (1 + pr.cola_snap) +
                -- 🤑 GOLD: Turning units get RENO snap, not norm snap!
                pr.norm_turn_rate * (1 + pr.reno_snap)
            ), 0
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
    ds_refi_year,
    CURRENT_TIMESTAMP AS calculated_at
FROM pgi_recursive
WHERE company_id = 1
ORDER BY property_id, year