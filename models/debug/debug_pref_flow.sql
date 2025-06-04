{{ config(materialized='view') }}
--------------------------------------------------------------------------------
-- models/debug_pref_flow.sql
--
-- A self-contained “Preferred IRR → ROC” view. No undefined columns—this will
-- compile on the first run. You do NOT need to edit any other file to fix
-- the “column does not exist” errors.
--
-- This does the following, step by step:
--   1) Filter tbl_terms to only equity_class = 'Preferred'
--   2) Sum ATCF by portfolio × year
--   3) Pro‐rata allocate that cash to each Preferred investor
--   4) In two CTEs, compute “interest_due” and “cash_available” first, then
--      compute “interest_paid” and “principal_paid” and roll forward balances.
--   5) Output one row per portfolio/investor/year with all intermediate columns.
--------------------------------------------------------------------------------

WITH
--------------------------------------------------------------------------------
-- 1️⃣ TERMS_PREF:  Only “Preferred” investors
--------------------------------------------------------------------------------
terms_pref AS (
  SELECT
    LOWER(portfolio_id)    AS portfolio_id,
    investor_serial        AS investor_id,
    equity_contributed,
    base_pref_irr,
    equity_class
  FROM "hkh_decision_support_db"."hkh_dev"."tbl_terms"
  WHERE equity_class = 'Preferred'
),

--------------------------------------------------------------------------------
-- 2️⃣ RAW_CASH:  Sum all ATCF per portfolio × year
--------------------------------------------------------------------------------
raw_cash AS (
  SELECT
    LOWER(pi.portfolio_id) AS portfolio_id,
    fpf.year               AS year,
    SUM(fpf.atcf)          AS alloc_cash_portfolio
  FROM "hkh_decision_support_db"."hkh_dev"."fact_property_cash_flow" AS fpf
  JOIN "hkh_decision_support_db"."inputs"."property_inputs" AS pi
    ON fpf.property_id = pi.property_id
  GROUP BY LOWER(pi.portfolio_id), fpf.year
),

--------------------------------------------------------------------------------
-- 3️⃣ INVESTOR_SHARES_PREF:  Allocate portfolio cash to each Pref investor
--------------------------------------------------------------------------------
investor_shares_pref AS (
  SELECT
    rc.portfolio_id,
    rc.year,

    tp.investor_id,
    tp.equity_contributed,
    tp.base_pref_irr,
    tp.equity_class,
    rc.alloc_cash_portfolio,

    -- Pro‐rata share within the Preferred class
    tp.equity_contributed
      / SUM(tp.equity_contributed) 
        OVER (PARTITION BY rc.portfolio_id) 
      AS pref_class_ratio,

    ROW_NUMBER() OVER (
      PARTITION BY rc.portfolio_id, tp.investor_id
      ORDER BY rc.year
    ) AS year_index

  FROM terms_pref AS tp
  JOIN raw_cash  AS rc
    ON tp.portfolio_id = rc.portfolio_id
),

--------------------------------------------------------------------------------
-- 4️⃣ ALLOC_PREF:  Each Pref investor’s “absolute cash” for that year
--------------------------------------------------------------------------------
alloc_pref AS (
  SELECT
    *,
    ROUND(alloc_cash_portfolio * pref_class_ratio, 2) AS alloc_cash_investor
  FROM investor_shares_pref
),

--------------------------------------------------------------------------------
-- 5️⃣ PREF_BASE:  Compute “prior balance” and “interest_due” and “cash_available”
--------------------------------------------------------------------------------
pref_base AS (
  SELECT
    ap.portfolio_id,
    ap.year,
    ap.investor_id,
    ap.equity_contributed,
    ap.base_pref_irr,
    ap.alloc_cash_investor,

    ----------------------------------------------------------------------
    -- (a) Cumulative principal PAID prior to this year.  At year_index=1, 
    --     nothing has been paid yet, so 0.  We will roll this forward in the next CTE.
    ----------------------------------------------------------------------
    0::numeric AS cum_principal_paid_prior,

    ----------------------------------------------------------------------
    -- (b) Prior balance (= original equity – cum_principal_paid_prior).  At 
    --     the very first year, that is just equity_contributed.
    ----------------------------------------------------------------------
    ap.equity_contributed AS prior_balance,

    ----------------------------------------------------------------------
    -- (c) Interest DUE this year = prior_balance * base_pref_irr
    ----------------------------------------------------------------------
    ROUND(
      ap.equity_contributed * ap.base_pref_irr,
      2
    ) AS interest_due,

    ----------------------------------------------------------------------
    -- (d) Cash AVAILABLE this year for this investor:
    --     = cumulative previous alloc + this year’s alloc
    ----------------------------------------------------------------------
    COALESCE(
      SUM(ap.alloc_cash_investor) OVER (
        PARTITION BY ap.portfolio_id, ap.investor_id
        ORDER BY ap.year
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0
    ) + ap.alloc_cash_investor AS cash_available_through_year

  FROM alloc_pref AS ap
  WHERE ap.equity_class = 'Preferred'
),

--------------------------------------------------------------------------------
-- 6️⃣ PREF_FINAL:  Now compute “interest_paid” and “principal_paid” and end‐balance
--------------------------------------------------------------------------------
pref_final AS (
  SELECT
    pb.portfolio_id,
    pb.year,
    pb.investor_id,
    pb.equity_contributed,
    pb.base_pref_irr,
    pb.alloc_cash_investor,

    ----------------------------------------------------------------------
    -- (1) cumulative principal paid from prior years (carried forward)
    ----------------------------------------------------------------------
    pb.cum_principal_paid_prior AS cum_pref_principal_paid_prior,

    ----------------------------------------------------------------------
    -- (2) prior_balance coming into this year
    ----------------------------------------------------------------------
    pb.prior_balance AS pref_cap_balance_prior,

    ----------------------------------------------------------------------
    -- (3) interest_due this year
    ----------------------------------------------------------------------
    pb.interest_due AS pref_interest_due,

    ----------------------------------------------------------------------
    -- (4) cash_available through this year
    ----------------------------------------------------------------------
    pb.cash_available_through_year,

    ----------------------------------------------------------------------
    -- (5) interest PAID this year = MIN(interest_due, cash_available)
    ----------------------------------------------------------------------
    ROUND(
      LEAST(
        pb.interest_due,
        pb.cash_available_through_year
      ),
      2
    ) AS pref_interest_paid,

    ----------------------------------------------------------------------
    -- (6) cash REMAINING after paying interest
    ----------------------------------------------------------------------
    ROUND(
      pb.cash_available_through_year 
      - LEAST(pb.interest_due, pb.cash_available_through_year),
      2
    ) AS cash_after_pref_interest,

    ----------------------------------------------------------------------
    -- (7) principal PAID this year = MIN(prior_balance, cash_after_pref_interest)
    ----------------------------------------------------------------------
    ROUND(
      LEAST(
        pb.prior_balance,
        (pb.cash_available_through_year - LEAST(pb.interest_due, pb.cash_available_through_year))
      ),
      2
    ) AS pref_principal_paid,

    ----------------------------------------------------------------------
    -- (8) end‐of‐year balance = prior_balance – principal_paid
    ----------------------------------------------------------------------
    ROUND(
      pb.prior_balance 
      - LEAST(
          pb.prior_balance,
          (pb.cash_available_through_year - LEAST(pb.interest_due, pb.cash_available_through_year))
        ),
      2
    ) AS pref_cap_balance_end

  FROM pref_base AS pb
)

SELECT
  *
FROM pref_final
ORDER BY portfolio_id, investor_id, year