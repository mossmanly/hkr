{{ config(materialized='view') }}
--------------------------------------------------------------------------------
-- models/debug/debug_pref_roc_irr.sql
--
-- Preferred “ROC first → IRR second” model (no Common, no Profit).
-- Each year:
--   1) Pay as much principal (ROC) as alloc_cash allows: 
--      principal_paid = MIN(prior_balance, alloc_cash_investor).
--   2) Compute balance_after_principal = prior_balance – principal_paid.
--   3) Compute interest_due = balance_after_principal × base_pref_irr.
--   4) Pay interest = MIN(interest_due, cash_remaining_after_principal).
--   5) end_balance = balance_after_principal (interest does not reduce the capital).
--------------------------------------------------------------------------------

WITH
--------------------------------------------------------------------------------
-- 1️⃣  TERMS_PREF:  Only Preferred investors
--------------------------------------------------------------------------------
terms_pref AS (
  SELECT
    LOWER(portfolio_id)    AS portfolio_id,
    investor_serial        AS investor_id,
    equity_contributed,
    base_pref_irr,         -- e.g. 0.07 for 7%
    equity_class
  FROM "hkh_decision_support_db"."hkh_dev"."tbl_terms"
  WHERE equity_class = 'Preferred'
),

--------------------------------------------------------------------------------
-- 2️⃣  RAW_CASH:  Sum all ATCF per portfolio × year
--------------------------------------------------------------------------------
raw_cash AS (
  SELECT
    LOWER(pi.portfolio_id) AS portfolio_id,
    fpf.year               AS year,
    SUM(fpf.atcf)          AS portfolio_cash
  FROM "hkh_decision_support_db"."hkh_dev"."fact_property_cash_flow" AS fpf
  JOIN "hkh_decision_support_db"."inputs"."property_inputs" AS pi
    ON fpf.property_id = pi.property_id
  GROUP BY LOWER(pi.portfolio_id), fpf.year
),

--------------------------------------------------------------------------------
-- 3️⃣  SHARES_PREF:  Allocate each year’s cash among Preferred (pro‐rata)
--------------------------------------------------------------------------------
shares_pref AS (
  SELECT
    rc.portfolio_id,
    rc.year,

    tp.investor_id,
    tp.equity_contributed,
    tp.base_pref_irr,

    rc.portfolio_cash,

    -- Pro‐rata share among all Preferred investors in this portfolio
    tp.equity_contributed
      / SUM(tp.equity_contributed) 
        OVER (PARTITION BY rc.portfolio_id) 
      AS pref_pct,

    ROW_NUMBER() OVER (
      PARTITION BY rc.portfolio_id, tp.investor_id 
      ORDER BY rc.year
    ) AS year_index

  FROM terms_pref AS tp
  JOIN raw_cash  AS rc
    ON tp.portfolio_id = rc.portfolio_id
),

--------------------------------------------------------------------------------
-- 4️⃣  ALLOC_PREF:  Compute $ allocated to each Preferred investor per year
--------------------------------------------------------------------------------
alloc_pref AS (
  SELECT
    sp.*,
    ROUND(sp.portfolio_cash * sp.pref_pct, 2) AS alloc_cash_investor
  FROM shares_pref AS sp
),

--------------------------------------------------------------------------------
-- 5️⃣  PREF_PRINCIPAL:  Compute “how much principal each year” and “balance after principal”
--------------------------------------------------------------------------------
pref_principal AS (
  SELECT
    ap.portfolio_id,
    ap.year,
    ap.investor_id,
    ap.equity_contributed,
    ap.base_pref_irr,
    ap.alloc_cash_investor,
    ap.year_index,

    ----------------------------------------------------------------
    -- (a) cumulative alloc through this year
    ----------------------------------------------------------------
    SUM(ap.alloc_cash_investor) 
      OVER (
        PARTITION BY ap.portfolio_id, ap.investor_id
        ORDER BY ap.year
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS cum_alloc_through_year,

    ----------------------------------------------------------------
    -- (b) cumulative principal paid through this year =
    --     MIN(equity_contributed, cum_alloc_through_year)
    ----------------------------------------------------------------
    LEAST(
      ap.equity_contributed,
      SUM(ap.alloc_cash_investor) 
        OVER (
          PARTITION BY ap.portfolio_id, ap.investor_id
          ORDER BY ap.year
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        )
    ) AS cum_principal_paid_now,

    ----------------------------------------------------------------
    -- (c) balance AFTER principal this year:
    --     = equity_contributed – cum_principal_paid_now
    ----------------------------------------------------------------
    ap.equity_contributed
    - LEAST(
        ap.equity_contributed,
        SUM(ap.alloc_cash_investor) 
          OVER (
            PARTITION BY ap.portfolio_id, ap.investor_id
            ORDER BY ap.year
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
          )
      ) AS balance_after_principal

  FROM alloc_pref AS ap
),

--------------------------------------------------------------------------------
-- 6️⃣  PREF_FLOW:  Determine ROC paid this year, then compute IRR on the remaining balance
--------------------------------------------------------------------------------
pref_flow AS (
  SELECT
    pp.portfolio_id,
    pp.year,
    pp.investor_id,
    pp.equity_contributed,
    pp.base_pref_irr,
    pp.alloc_cash_investor,
    pp.year_index,

    --------------------------------------------------------------------
    -- (1) Cumulative principal paid PRIOR to this year
    --------------------------------------------------------------------
    COALESCE(
      LAG(pp.cum_principal_paid_now) 
      OVER (
        PARTITION BY pp.portfolio_id, pp.investor_id
        ORDER BY pp.year
      ),
      0
    ) AS cum_principal_paid_prior,

    --------------------------------------------------------------------
    -- (2) Pref principal (ROC) paid THIS YEAR =
    --     = cum_principal_paid_now – cum_principal_paid_prior
    --------------------------------------------------------------------
    ROUND(
      pp.cum_principal_paid_now
      - COALESCE(
          LAG(pp.cum_principal_paid_now)
          OVER (
            PARTITION BY pp.portfolio_id, pp.investor_id
            ORDER BY pp.year
          ),
          0
        ),
      2
    ) AS pref_principal_paid,

    --------------------------------------------------------------------
    -- (3) Cash remaining AFTER paying this year’s principal
    --     = alloc_cash_investor – pref_principal_paid
    --------------------------------------------------------------------
    ROUND(
      pp.alloc_cash_investor
      - (
          pp.cum_principal_paid_now 
          - COALESCE(
              LAG(pp.cum_principal_paid_now)
              OVER (
                PARTITION BY pp.portfolio_id, pp.investor_id
                ORDER BY pp.year
              ),
              0
            )
        ),
      2
    ) AS cash_after_pref_principal,

    --------------------------------------------------------------------
    -- (4) PRIOR balance AFTER principal = lag(balance_after_principal)
    --------------------------------------------------------------------
    COALESCE(
      LAG(pp.balance_after_principal) 
      OVER (
        PARTITION BY pp.portfolio_id, pp.investor_id
        ORDER BY pp.year
      ),
      pp.equity_contributed
    ) AS prior_balance_after_principal,

    --------------------------------------------------------------------
    -- (5) Pref interest DUE this year 
    --     = prior_balance_after_principal × base_pref_irr
    --------------------------------------------------------------------
    ROUND(
      (
        COALESCE(
          LAG(pp.balance_after_principal)
          OVER (
            PARTITION BY pp.portfolio_id, pp.investor_id
            ORDER BY pp.year
          ),
          pp.equity_contributed
        )
      ) * pp.base_pref_irr,
      2
    ) AS pref_interest_due,

    --------------------------------------------------------------------
    -- (6) Pref interest PAID this year 
    --     = MIN(pref_interest_due, cash_after_pref_principal)
    --------------------------------------------------------------------
    ROUND(
      LEAST(
        ROUND(
          (
            COALESCE(
              LAG(pp.balance_after_principal)
              OVER (
                PARTITION BY pp.portfolio_id, pp.investor_id
                ORDER BY pp.year
              ),
              pp.equity_contributed
            ) * pp.base_pref_irr
          ),
          2
        ),
        (
          pp.alloc_cash_investor
          - (
              pp.cum_principal_paid_now 
              - COALESCE(
                  LAG(pp.cum_principal_paid_now)
                  OVER (
                    PARTITION BY pp.portfolio_id, pp.investor_id
                    ORDER BY pp.year
                  ),
                  0
                )
            )
        )
      ),
      2
    ) AS pref_interest_paid,

    --------------------------------------------------------------------
    -- (7) END_BALANCE (after principal only; IRR does not reduce capital)
    --------------------------------------------------------------------
    ROUND(
      COALESCE(
        LAG(pp.balance_after_principal)
        OVER (
          PARTITION BY pp.portfolio_id, pp.investor_id
          ORDER BY pp.year
        ),
        pp.equity_contributed
      )
      - (
          pp.cum_principal_paid_now 
          - COALESCE(
              LAG(pp.cum_principal_paid_now)
              OVER (
                PARTITION BY pp.portfolio_id, pp.investor_id
                ORDER BY pp.year
              ),
              0
            )
        ),
      2
    ) AS end_balance

  FROM pref_principal AS pp
)

--------------------------------------------------------------------------------
-- FINAL OUTPUT: One row per Pref investor × year, showing exactly:
--   • alloc_cash_investor 
--   • prior_balance_after_principal 
--   • pref_principal_paid 
--   • cash_after_pref_principal 
--   • pref_interest_due 
--   • pref_interest_paid 
--   • end_balance
--------------------------------------------------------------------------------
SELECT
  pf.portfolio_id                        AS portfolio_id,
  pf.year                                AS year,
  pf.investor_id                         AS investor_id,

  /* 1) Allocated cash to Pref investor this year  */
  pf.alloc_cash_investor                  AS alloc_cash,

  /* 2) Balance AFTER principal from last year  */
  pf.prior_balance_after_principal        AS prior_balance,

  /* 3) Pref principal (ROC) paid this year  */
  pf.pref_principal_paid                  AS principal_paid,

  /* 4) Cash leftover after paying principal  */
  pf.cash_after_pref_principal             AS cash_after_principal,

  /* 5) Pref interest DUE this year  */
  pf.pref_interest_due                     AS interest_due,

  /* 6) Pref interest PAID this year  */
  pf.pref_interest_paid                    AS interest_paid,

  /* 7) End capital balance after principal (interest does not reduce capital)  */
  pf.end_balance                           AS end_balance

FROM pref_flow AS pf
ORDER BY pf.portfolio_id, pf.investor_id, pf.year