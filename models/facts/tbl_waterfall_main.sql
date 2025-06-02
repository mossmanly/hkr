{{ config(materialized = 'view') }}

WITH

-------------------------------------------------------------------
-- 1) AGGREGATE PROPERTY CASH (ATCF) BY PORTFOLIO × YEAR
-------------------------------------------------------------------
portfolio_cash AS (
    SELECT
        pi.portfolio_id,
        fpf.year,
        SUM(fpf.atcf) AS distributable_cash
    FROM {{ ref('fact_property_cash_flow') }}       AS fpf
    LEFT JOIN {{ source('inputs', 'property_inputs') }} AS pi
      ON fpf.property_id = pi.property_id
    GROUP BY pi.portfolio_id, fpf.year
),

-------------------------------------------------------------------
-- 2) AGGREGATE EQUITY & WEIGHTED BASE PREF IRR
-------------------------------------------------------------------
equity AS (
    SELECT
        t.portfolio_id,
        SUM(CASE WHEN t.equity_class = 'Preferred' THEN t.equity_contributed ELSE 0 END) AS pref_equity,
        SUM(CASE WHEN t.equity_class = 'Common'    THEN t.equity_contributed ELSE 0 END) AS common_equity,
        CASE
            WHEN SUM(t.equity_contributed) = 0 THEN 0
            ELSE ROUND(
                     SUM(t.base_pref_irr * t.equity_contributed)
                   / SUM(t.equity_contributed),
                   4
                 )
        END AS base_pref_irr
    FROM {{ source('hkh_dev', 'tbl_terms') }} AS t
    GROUP BY t.portfolio_id
),

-------------------------------------------------------------------
-- 3) JOIN CASH + EQUITY
-------------------------------------------------------------------
joined AS (
    SELECT
        pc.portfolio_id,
        pc.year,
        pc.distributable_cash,
        COALESCE(e.pref_equity,  0) AS pref_equity,
        COALESCE(e.common_equity, 0) AS common_equity,
        COALESCE(e.base_pref_irr, 0) AS base_pref_irr,
        COALESCE(e.pref_equity,  0) AS total_pref_capital,
        COALESCE(e.common_equity, 0) AS total_common_capital
    FROM portfolio_cash pc
    LEFT JOIN equity e
      ON REPLACE(pc.portfolio_id, '-', '_') = e.portfolio_id
),

-------------------------------------------------------------------
-- 4) CUMULATIVE CASH
-------------------------------------------------------------------
prep AS (
    SELECT
        *,
        SUM(distributable_cash)
          OVER (PARTITION BY portfolio_id ORDER BY year) AS cumulative_cash
    FROM joined
),

-------------------------------------------------------------------
-- 5) PREFERRED ROC
-------------------------------------------------------------------
pref_roc_step AS (
    SELECT
        *,
        LEAST(cumulative_cash, total_pref_capital)                     AS cumulative_pref_roc
    FROM prep
),
pref_roc AS (
    SELECT
        *,
        cumulative_pref_roc
          - LAG(cumulative_pref_roc, 1, 0)
              OVER (PARTITION BY portfolio_id ORDER BY year)           AS pref_roc
    FROM pref_roc_step
),

-------------------------------------------------------------------
-- 6) CASH AFTER PREF ROC
-------------------------------------------------------------------
post_pref_roc AS (
    SELECT
        *,
        cumulative_cash - cumulative_pref_roc                          AS cumulative_cash_after_pref_roc
    FROM pref_roc
),

-------------------------------------------------------------------
-- 7) PREF IRR OWED & PAID
-------------------------------------------------------------------
pref_irr_calc AS (
    SELECT
        *,
        GREATEST(0, total_pref_capital - cumulative_pref_roc)          AS outstanding_pref_bal,
        ROUND(GREATEST(0, total_pref_capital - cumulative_pref_roc) 
              * base_pref_irr, 2)                                      AS pref_owed_this_year
    FROM post_pref_roc
),
pref_irr_paid AS (
    SELECT
        *,
        CASE
            WHEN cumulative_cash_after_pref_roc >= pref_owed_this_year
              THEN pref_owed_this_year
            ELSE cumulative_cash_after_pref_roc
        END                                                            AS pref_irr,
        SUM(
            CASE
                WHEN cumulative_cash_after_pref_roc >= pref_owed_this_year
                  THEN pref_owed_this_year
                ELSE cumulative_cash_after_pref_roc
            END
        ) OVER (PARTITION BY portfolio_id ORDER BY year)               AS cumulative_pref_irr_paid
    FROM pref_irr_calc
),

-------------------------------------------------------------------
-- 8) CASH AFTER ALL PREF PAYMENTS
-------------------------------------------------------------------
post_pref_all AS (
    SELECT
        *,
        cumulative_cash
          - cumulative_pref_roc
          - cumulative_pref_irr_paid                                   AS cumulative_cash_after_pref_all
    FROM pref_irr_paid
),

-------------------------------------------------------------------
-- 9) COMMON ROC
-------------------------------------------------------------------
common_roc_step AS (
    SELECT
        *,
        LEAST(cumulative_cash_after_pref_all, total_common_capital)    AS cumulative_common_roc
    FROM post_pref_all
),
common_roc AS (
    SELECT
        *,
        cumulative_common_roc
          - LAG(cumulative_common_roc, 1, 0)
              OVER (PARTITION BY portfolio_id ORDER BY year)           AS common_roc
    FROM common_roc_step
),

-------------------------------------------------------------------
-- 10) COMMON IRR OWED & PAID
-------------------------------------------------------------------
common_irr_calc AS (
    SELECT
        *,
        GREATEST(0, total_common_capital - cumulative_common_roc)      AS outstanding_common_bal,
        ROUND(GREATEST(0, total_common_capital - cumulative_common_roc)
              * base_pref_irr, 2)                                      AS common_owed_this_year
    FROM common_roc
),
common_irr_paid AS (
    SELECT
        *,
        CASE
            WHEN (cumulative_cash_after_pref_all - cumulative_common_roc) >= common_owed_this_year
              THEN common_owed_this_year
            ELSE GREATEST(0, cumulative_cash_after_pref_all - cumulative_common_roc)
        END                                                            AS common_irr,
        SUM(
            CASE
                WHEN (cumulative_cash_after_pref_all - cumulative_common_roc) >= common_owed_this_year
                  THEN common_owed_this_year
                ELSE GREATEST(0, cumulative_cash_after_pref_all - cumulative_common_roc)
            END
        ) OVER (PARTITION BY portfolio_id ORDER BY year)               AS cumulative_common_irr_paid
    FROM common_irr_calc
)

-------------------------------------------------------------------
-- 11) FINAL SELECT
-------------------------------------------------------------------
SELECT
    'hkh'                                             AS company_id,
    portfolio_id,
    year,
    distributable_cash,

    pref_roc,
    pref_irr,
    common_roc,
    common_irr,

    /* sponsor leftover */
    GREATEST(
        0,
        distributable_cash - (pref_roc + pref_irr + common_roc + common_irr)
    )                                                 AS distributable_profit,

    /* future hurdle placeholders */
    0::numeric AS hurdle1_total, 0::numeric AS hurdle1_common, 0::numeric AS hurdle1_sponsor,
    0::numeric AS hurdle2_total, 0::numeric AS hurdle2_common, 0::numeric AS hurdle2_sponsor,
    0::numeric AS hurdle3_total, 0::numeric AS hurdle3_common, 0::numeric AS hurdle3_sponsor,
    0::numeric AS residual_total, 0::numeric AS residual_common, 0::numeric AS residual_sponsor,

    (common_roc + common_irr)                        AS common_total,
    GREATEST(
        0,
        distributable_cash - (pref_roc + pref_irr + common_roc + common_irr)
    )                                                 AS sponsor_total
FROM common_irr_paid