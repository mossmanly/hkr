{{ config(materialized = 'view') }}

WITH
/* ───────────────────────────── 1 • cash & equity ───────────────────────────── */
portfolio_cash AS (
    SELECT
        pi.portfolio_id,
        fpf.year,
        SUM(fpf.atcf) AS distributable_cash
    FROM {{ ref('fact_property_cash_flow') }} fpf
    LEFT JOIN {{ source('inputs','property_inputs') }} pi
           ON fpf.property_id = pi.property_id
    GROUP BY pi.portfolio_id, fpf.year
),
equity AS (
    SELECT
        t.portfolio_id,
        SUM(CASE WHEN t.equity_class='Preferred' THEN t.equity_contributed ELSE 0 END) AS pref_equity,
        SUM(CASE WHEN t.equity_class='Common'    THEN t.equity_contributed ELSE 0 END) AS common_equity,
        ROUND(
          SUM(t.base_pref_irr * t.equity_contributed)
          / NULLIF(SUM(t.equity_contributed),0), 4
        ) AS base_pref_irr
    FROM {{ source('hkh_dev','tbl_terms') }} t
    GROUP BY t.portfolio_id
),
joined AS (
    SELECT
        pc.*,
        e.pref_equity,
        e.common_equity,
        e.base_pref_irr,
        e.pref_equity                    AS total_pref_capital,
        e.common_equity                  AS total_common_capital,
        e.pref_equity + e.common_equity  AS total_equity
    FROM portfolio_cash pc
    LEFT JOIN equity e
      ON REPLACE(pc.portfolio_id,'-','_') = e.portfolio_id
),
cash_cum AS (
    SELECT
        j.*,
        SUM(distributable_cash) OVER (PARTITION BY portfolio_id ORDER BY year) AS cum_cash
    FROM joined j
),

/* ───────────────────────────── 2 • preferred ROC & cumulative ──────────────── */
pref_cum AS (
    SELECT
        *,
        LEAST(cum_cash,total_pref_capital) AS cum_pref_roc
    FROM cash_cum
),
pref_roc AS (
    SELECT
        *,
        cum_pref_roc
        - COALESCE(LAG(cum_pref_roc) OVER (PARTITION BY portfolio_id ORDER BY year),0)
        AS pref_roc
    FROM pref_cum
),

/* ───────────────────────────── 3 • preferred IRR accrual --------------------- */
pref_balance AS (
    SELECT
        *,
        total_pref_capital - cum_pref_roc AS pref_outstanding
    FROM pref_roc
),
pref_irr_paid AS (
    SELECT
        *,
        ROUND(pref_outstanding * base_pref_irr,2) AS pref_irr
    FROM pref_balance
),

/* ───────────────────────────── 4 • common ROC & cumulative ─────────────────── */
common_cum AS (
    SELECT
        *,
        LEAST(
          cum_cash
          - cum_pref_roc,                -- cash net of pref ROC
          total_common_capital
        ) AS cum_common_roc
    FROM pref_irr_paid
),
common_roc AS (
    SELECT
        *,
        cum_common_roc
        - COALESCE(LAG(cum_common_roc) OVER (PARTITION BY portfolio_id ORDER BY year),0)
        AS common_roc
    FROM common_cum
),

/* ───────────────────────────── 5 • common IRR accrual ------------------------ */
common_balance AS (
    SELECT
        *,
        total_common_capital - cum_common_roc AS common_outstanding
    FROM common_roc
),
common_irr_paid AS (
    SELECT
        *,
        ROUND(common_outstanding * base_pref_irr,2) AS common_irr
    FROM common_balance
),

/* ───────────────────────────── 6 • running IRR & distributable profit -------- */
running AS (
    SELECT
        *,
        distributable_cash
          - pref_roc - pref_irr
          - common_roc - common_irr                  AS distributable_profit,

        CASE
          WHEN year = 0 THEN NULL
          ELSE POWER(
                 1 + (
                   cum_pref_roc + cum_common_roc
                   + SUM(pref_irr + common_irr)
                       OVER (PARTITION BY portfolio_id ORDER BY year)
                 ) / NULLIF(total_equity,0),
                 1.0 / year
               ) - 1
        END AS wtd_running_irr
    FROM common_irr_paid
),

/* ───────────────────────────── 7 • hurdle-1 promote split -------------------- */
h1 AS (
    SELECT * FROM {{ source('hkh_dev','tbl_hurdle_tiers') }}
    WHERE hurdle_id = 'hurdle1'
),
split AS (
    SELECT
        r.*,
        h1.common_share,
        h1.sponsor_share,
        ROUND(distributable_profit * h1.common_share,2)  AS hurdle_common,
        ROUND(distributable_profit * h1.sponsor_share,2) AS hurdle_sponsor
    FROM running r
    CROSS JOIN h1
)

/* ───────────────────────────── 8 • final view ------------------------------- */
SELECT
    'hkh' AS company_id,
    portfolio_id,
    year,
    distributable_cash,

    pref_roc,
    pref_irr,
    common_roc,
    common_irr,

    distributable_profit,

    (hurdle_common + hurdle_sponsor)      AS hurdle1_total,
    hurdle_common                         AS hurdle1_common,
    hurdle_sponsor                        AS hurdle1_sponsor,

    0::numeric AS hurdle2_total,
    0::numeric AS hurdle2_common,
    0::numeric AS hurdle2_sponsor,
    0::numeric AS hurdle3_total,
    0::numeric AS hurdle3_common,
    0::numeric AS hurdle3_sponsor,
    0::numeric AS residual_total,
    0::numeric AS residual_common,
    0::numeric AS residual_sponsor,

    (common_roc + common_irr + hurdle_common) AS common_total,
    hurdle_sponsor                            AS sponsor_total,

    wtd_running_irr

FROM split
ORDER BY portfolio_id, year;