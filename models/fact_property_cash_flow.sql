-- models/fact_property_cash_flow.sql

WITH assumptions AS (
    SELECT
        property_id,
        unit_count,
        avg_rent_per_unit,
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

years AS (
    SELECT generate_series(1, 20) AS year
),

pgi_calc AS (
    SELECT
        a.property_id,
        y.year,
        ROUND(a.unit_count * a.avg_rent_per_unit * 12 * POWER(1.03, y.year - 1), 2) AS pgi,
        a.vacancy_rate,
        a.collections_loss_rate,
        a.opex_ratio,
        a.unit_count,
        a.capex_per_unit,
        a.ds_refi_year
    FROM assumptions a
    CROSS JOIN years y
),

original_ds AS (
    SELECT 
        property_id,
        ROUND(purchase_price * ds_ltv, 2) AS loan_amount,
        ROUND(
            (purchase_price * ds_ltv)
            * (ds_int * POWER(1 + ds_int, ds_term))
            / (POWER(1 + ds_int, ds_term) - 1),
            2
        ) AS annual_ds
    FROM {{ source('inputs', 'property_inputs') }}
),

capex_calc AS (
    SELECT
        f.property_id,
        f.year,
        ROUND(rra.unit_count * rra.capex_per_unit * f.capex_factor, 2) AS capex
    FROM {{ source('inputs', 'capex_factors') }} f
    JOIN {{ source('inputs', 'property_inputs') }} rra ON rra.property_id = f.property_id
),

refi AS (
    SELECT
        property_id,
        ds_refi_year,
        refi_annual_ds,
        refi_proceeds
    FROM {{ ref('refi_outcomes') }}
)

SELECT
    'micro-1' AS portfolio_id,  -- Added portfolio_id column
    pc.property_id,
    pc.year,
    pc.pgi,
    ROUND(pc.pgi * COALESCE(pc.vacancy_rate, 0), 2) AS vacancy_loss,
    ROUND((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0)) * COALESCE(pc.collections_loss_rate, 0), 2) AS collections_loss,
    ROUND(
        pc.pgi
        - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
        - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0),
        2
    ) AS egi,
    ROUND(
        (
            pc.pgi
            - (pc.pgi * COALESCE(pc.vacancy_rate, 0))
            - ((pc.pgi - pc.pgi * COALESCE(pc.vacancy_rate, 0))) * COALESCE(pc.collections_loss_rate, 0)
        ) * COALESCE(pc.opex_ratio, 0.30),
        2
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
        2
    ) AS noi,
    ROUND(
        CASE
            WHEN pc.year < pi.ds_refi_year THEN ods.annual_ds
            ELSE ro.refi_annual_ds
        END,
        2
    ) AS debt_service,
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
        ) - (
            CASE
                WHEN pc.year < pi.ds_refi_year THEN ods.annual_ds
                ELSE ro.refi_annual_ds
            END
        ),
        2
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
            ) - (
                CASE
                    WHEN pc.year < pi.ds_refi_year THEN ods.annual_ds
                    ELSE ro.refi_annual_ds
                END
            )
        ) - COALESCE(cx.capex, 0)
        + CASE WHEN pc.year = pi.ds_refi_year THEN COALESCE(ro.refi_proceeds, 0) ELSE 0 END,
        2
    ) AS atcf

FROM pgi_calc pc
LEFT JOIN assumptions pi ON pi.property_id = pc.property_id
LEFT JOIN original_ds ods ON ods.property_id = pc.property_id
LEFT JOIN capex_calc cx ON cx.property_id = pc.property_id AND cx.year = pc.year
LEFT JOIN refi ro ON ro.property_id = pc.property_id