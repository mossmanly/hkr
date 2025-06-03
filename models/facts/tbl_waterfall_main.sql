{{ config(materialized = 'view') }}

WITH
/* ───────────────────────────────────────────────────────────────┐
   1) BASE WATERFALL (unchanged tiers 1-4: pref ROC / IRR / etc.)
   ───────────────────────────────────────────────────────────────┘ */
-- portfolio cash --------------------------
portfolio_cash AS (
    SELECT
        pi.portfolio_id,
        fpf.year,
        SUM(fpf.atcf) AS distributable_cash
    FROM {{ ref('fact_property_cash_flow') }} fpf
    LEFT JOIN {{ source('inputs', 'property_inputs') }} pi
      ON fpf.property_id = pi.property_id
    GROUP BY pi.portfolio_id, fpf.year
),

-- equity & weighted base_pref_irr ----------
equity AS (
    SELECT
        t.portfolio_id,
        SUM(CASE WHEN t.equity_class='Preferred' THEN t.equity_contributed ELSE 0 END) AS pref_equity,
        SUM(CASE WHEN t.equity_class='Common'    THEN t.equity_contributed ELSE 0 END) AS common_equity,
        ROUND(
            SUM(t.base_pref_irr * t.equity_contributed) / NULLIF(SUM(t.equity_contributed),0)
        ,4) AS base_pref_irr
    FROM {{ source('hkh_dev','tbl_terms') }} t
    GROUP BY t.portfolio_id
),

joined AS (
    SELECT
        pc.*,
        COALESCE(e.pref_equity,0)   AS pref_equity,
        COALESCE(e.common_equity,0) AS common_equity,
        COALESCE(e.base_pref_irr,0) AS base_pref_irr,
        COALESCE(e.pref_equity,0)   AS total_pref_capital,
        COALESCE(e.common_equity,0) AS total_common_capital,
        COALESCE(e.pref_equity,0)+COALESCE(e.common_equity,0) AS total_equity
    FROM portfolio_cash pc
    LEFT JOIN equity e
      ON REPLACE(pc.portfolio_id,'-','_') = e.portfolio_id
),

prep AS (
    SELECT
        *,
        SUM(distributable_cash) OVER (PARTITION BY portfolio_id ORDER BY year) AS cumulative_cash
    FROM joined
),

-- preferred ROC --------------------------------
pref_roc_step AS (
    SELECT *,
           LEAST(cumulative_cash,total_pref_capital)                     AS cumulative_pref_roc
    FROM prep
),
pref_roc AS (
    SELECT *,
           cumulative_pref_roc - LAG(cumulative_pref_roc,1,0)
             OVER (PARTITION BY portfolio_id ORDER BY year)              AS pref_roc
    FROM pref_roc_step
),

-- cash after pref ROC --------------------------
post_pref_roc AS (
    SELECT *,
           cumulative_cash - cumulative_pref_roc                         AS cumulative_cash_after_pref_roc
    FROM pref_roc
),

-- preferred IRR owed & paid --------------------
pref_irr_calc AS (
    SELECT *,
           GREATEST(0,total_pref_capital-cumulative_pref_roc)            AS pref_outstanding,
           ROUND(GREATEST(0,total_pref_capital-cumulative_pref_roc)*base_pref_irr,2)
                                                                         AS pref_due
    FROM post_pref_roc
),
pref_irr_paid AS (
    SELECT *,
           CASE WHEN cumulative_cash_after_pref_roc>=pref_due
                THEN pref_due
                ELSE cumulative_cash_after_pref_roc
           END                                                           AS pref_irr,
           SUM(
             CASE WHEN cumulative_cash_after_pref_roc>=pref_due
                  THEN pref_due
                  ELSE cumulative_cash_after_pref_roc
             END) OVER (PARTITION BY portfolio_id ORDER BY year)         AS cumulative_pref_irr
    FROM pref_irr_calc
),

post_pref_all AS (
    SELECT *,
           cumulative_cash
           - cumulative_pref_roc
           - cumulative_pref_irr                                         AS cumulative_cash_after_pref_all
    FROM pref_irr_paid
),

-- common ROC -----------------------------------
common_roc_step AS (
    SELECT *,
           LEAST(cumulative_cash_after_pref_all,total_common_capital)    AS cumulative_common_roc
    FROM post_pref_all
),
common_roc AS (
    SELECT *,
           cumulative_common_roc - LAG(cumulative_common_roc,1,0)
             OVER (PARTITION BY portfolio_id ORDER BY year)              AS common_roc
    FROM common_roc_step
),

-- common IRR owed & paid -----------------------
common_irr_calc AS (
    SELECT *,
           GREATEST(0,total_common_capital-cumulative_common_roc)        AS common_outstanding,
           ROUND(GREATEST(0,total_common_capital-cumulative_common_roc)*base_pref_irr,2)
                                                                         AS common_due
    FROM common_roc
),
common_irr_paid AS (
    SELECT *,
           CASE WHEN (cumulative_cash_after_pref_all-cumulative_common_roc)>=common_due
                THEN common_due
                ELSE GREATEST(0,cumulative_cash_after_pref_all-cumulative_common_roc)
           END                                                           AS common_irr,
           SUM(
               CASE WHEN (cumulative_cash_after_pref_all-cumulative_common_roc)>=common_due
                    THEN common_due
                    ELSE GREATEST(0,cumulative_cash_after_pref_all-cumulative_common_roc)
               END
           ) OVER (PARTITION BY portfolio_id ORDER BY year)              AS cumulative_common_irr
    FROM common_irr_calc
),

-- ───────────────────────────────────────────────
-- 2) RUNNING WEIGHTED-AVG INVESTOR IRR
--    (simple CAGR approximation)
-- ───────────────────────────────────────────────
running_irr AS (
    SELECT
        *,
        CASE
          WHEN year = 0 THEN NULL
          ELSE POWER(
                   NULLIF(
                     1 + (
                       (cumulative_pref_roc + cumulative_pref_irr + cumulative_common_roc + cumulative_common_irr)
                       / NULLIF(total_equity,0)
                     ),1
                   ),
                   1.0 / year
               ) - 1
        END AS wtd_running_irr
    FROM common_irr_paid
),

-- ───────────────────────────────────────────────
-- 3) JOIN TO HURDLE TIERS & CALCULATE SPLITS
-- ───────────────────────────────────────────────
hurdles AS (
    SELECT * FROM {{ source('hkh_dev','tbl_hurdle_tiers') }}
),

promote_split AS (
    /* join each row to the *first* hurdle whose ceiling hasn't been reached */
    SELECT
        r.*,
        h.hurdle_id,
        h.common_share,
        h.sponsor_share,
        CASE
          WHEN r.wtd_running_irr IS NULL
               OR r.wtd_running_irr < h.irr_range_high
          THEN ROUND(distributable_profit * h.common_share, 2)
          ELSE 0
        END AS hurdle_common,
        CASE
          WHEN r.wtd_running_irr IS NULL
               OR r.wtd_running_irr < h.irr_range_high
          THEN ROUND(distributable_profit * h.sponsor_share, 2)
          ELSE distributable_profit  -- 100 % sponsor after ceiling passed
        END AS hurdle_sponsor
    FROM running_irr r
    JOIN LATERAL (
        SELECT *
        FROM hurdles h
        WHERE r.wtd_running_irr IS NULL
              OR r.wtd_running_irr < h.irr_range_high
        ORDER BY h.irr_range_high
        LIMIT 1
    ) h ON TRUE
),

/* cumulative promote paid (optional – good for dashboards) */
cumulative_promote AS (
    SELECT
        *,
        SUM(hurdle_common)  OVER (PARTITION BY portfolio_id ORDER BY year) AS cumulative_hurdle_common,
        SUM(hurdle_sponsor) OVER (PARTITION BY portfolio_id ORDER BY year) AS cumulative_hurdle_sponsor
    FROM promote_split
)

/* ───────────────────────────────────────────────
   4) FINAL OUTPUT
   ─────────────────────────────────────────────── */
SELECT
    'hkh'  AS company_id,
    portfolio_id,
    year,
    distributable_cash,

    /* original tiers */
    pref_roc,
    pref_irr,
    common_roc,
    common_irr,

    /* promote tier of THIS year */
    hurdle_common      AS hurdle1_common,
    hurdle_sponsor     AS hurdle1_sponsor,

    /* leftover after this year’s split (should be zero) */
    0::numeric         AS hurdle1_total,   -- kept for backward-compat

    /* convenience */
    (common_roc + common_irr + hurdle_common)  AS common_total,
    hurdle_sponsor                              AS sponsor_total,

    /* running IRR visibility */
    wtd_running_irr

FROM cumulative_promote
ORDER BY portfolio_id, year;