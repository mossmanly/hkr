{{ config(materialized='view') }}

{#
  tbl_waterfall_main.sql

  This model builds a portfolio-level “waterfall” view. It computes:

    1) Investor-level Pref → ROC → Promote → Exit logic (purely to derive
       “distributable_profit” for each portfolio×year).

    2) Rolls up everything to portfolio×year, producing “distributable_profit”
       and a cumulative multiple on equity (cum_multiple_current).

    3) Dynamically joins into hurdle tiers (tbl_hurdle_tiers) based on the
       cumulative multiple, to find which hurdle each year falls into.

    4) Splits each year’s distributable_profit between ALL-investors (common)
       vs. sponsor, according to the matched hurdle’s percentages.

  NOTE:
    – We rely on dbt’s implicit wrapping: this file includes only the
      WITH … SELECT block.  dbt itself injects the CREATE VIEW for you.
    – There must be **no** explicit “CREATE VIEW …” or trailing semicolon here.
    – All of the “0.00 AS pref_… , 0.00 AS hurdle1_total, …” lines are
      *intentional placeholders* so that downstream models (e.g. investor_distributions)
      can reference them.  If these lines do not appear verbatim, the view
      will never expose those columns.
#}

WITH 
------------------------------------------------------------------------------
-- 1️⃣  TERMS: Pull in each investor’s equity, base_pref_irr, and target_irr
------------------------------------------------------------------------------
terms AS (
  SELECT
    LOWER(portfolio_id)       AS portfolio_id,
    investor_serial           AS investor_id,
    equity_contributed,
    base_pref_irr,
    target_irr     AS exit_irr,
    equity_class
  FROM {{ source('hkh_dev', 'tbl_terms') }}
),

------------------------------------------------------------------------------
-- 2️⃣  CASH: Aggregate total ATCF per PROPERTY → per PORTFOLIO × YEAR
------------------------------------------------------------------------------
raw_cash AS (
  SELECT
    LOWER(pi.portfolio_id)    AS portfolio_id,
    fpf.year                  AS year,
    SUM(fpf.atcf)             AS alloc_cash_portfolio
  FROM {{ source('hkh_dev', 'fact_property_cash_flow') }} AS fpf
  JOIN {{ source('inputs', 'property_inputs') }}    AS pi
    ON fpf.property_id = pi.property_id
  GROUP BY LOWER(pi.portfolio_id), fpf.year
),

------------------------------------------------------------------------------
-- 3️⃣  INVESTOR_SHARES: Allocate each year’s portfolio cash to each investor
--     by pro-rata share within their equity_class
------------------------------------------------------------------------------
investor_shares AS (
  SELECT
    rc.portfolio_id,
    rc.year,

    t.investor_id,
    t.equity_contributed,
    t.base_pref_irr,
    t.exit_irr,
    t.equity_class,

    rc.alloc_cash_portfolio,

    -- Investor’s share of this year’s portfolio cash, pro-rata within equity_class
    t.equity_contributed
      / SUM(t.equity_contributed) 
        OVER (PARTITION BY rc.portfolio_id, t.equity_class) 
      AS class_ratio,

    -- Year index per investor (1 = first year they see cash, 2 = second, etc.)
    ROW_NUMBER() OVER (
      PARTITION BY rc.portfolio_id, t.investor_id
      ORDER BY rc.year
    ) AS year_index

  FROM terms          AS t
  JOIN raw_cash      AS rc
    ON LOWER(t.portfolio_id) = rc.portfolio_id
),

------------------------------------------------------------------------------
-- 4️⃣  ALLOC_BY_INVESTOR: Materialize each investor’s slice of the portfolio cash
------------------------------------------------------------------------------
alloc_by_investor AS (
  SELECT
    *,
    ROUND(alloc_cash_portfolio * class_ratio, 2) AS alloc_cash_investor
  FROM investor_shares
),

------------------------------------------------------------------------------
-- 5️⃣  PREF_CALC: Compute how much Preferred is “due” and track cumulative cash
------------------------------------------------------------------------------
pref_calc AS (
  SELECT
    abi.*,

    -- Annual preferred due this year
    (equity_contributed * base_pref_irr) AS annual_pref_due,

    -- Cumulative preferred “due” through this year
    (equity_contributed * base_pref_irr) * year_index AS cum_pref_due,

    -- Cumulative cash available (alloc_cash_investor) through this year
    SUM(alloc_cash_investor) OVER (
      PARTITION BY portfolio_id, investor_id
      ORDER BY year
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cum_cash_available

  FROM alloc_by_investor AS abi
),

------------------------------------------------------------------------------
-- 6️⃣  PAID_PREF_CALC: Determine how much Preferred has been paid so far,
--     then pay this year’s Preferred up to the “due”
------------------------------------------------------------------------------
paid_pref_calc AS (
  SELECT
    pc.*,

    --------------------------------------------------------------------
    -- (a) Cumulative cash available PRIOR to this year (years < current)
    --------------------------------------------------------------------
    COALESCE(
      SUM(alloc_cash_investor) OVER (
        PARTITION BY portfolio_id, investor_id
        ORDER BY year
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS cum_cash_available_prior,

    --------------------------------------------------------------------
    -- (b) Cumulative Preferred due PRIOR to this year:
    --     = (equity_contributed * base_pref_irr) * (year_index - 1)
    --------------------------------------------------------------------
    (equity_contributed * base_pref_irr) * (year_index - 1) AS cum_pref_due_prior,

    --------------------------------------------------------------------
    -- (c) Cumulative Preferred PAID PRIOR to this year:
    --     = LEAST(cum_cash_available_prior, cum_pref_due_prior)
    --------------------------------------------------------------------
    LEAST(
      COALESCE(
        SUM(alloc_cash_investor) OVER (
          PARTITION BY portfolio_id, investor_id
          ORDER BY year
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        0
      ),
      (equity_contributed * base_pref_irr) * (year_index - 1)
    ) AS cum_pref_paid_prior,

    --------------------------------------------------------------------
    -- (d) Preferred balance STILL OWED before this year:
    --     = cum_pref_due_prior - cum_pref_paid_prior
    --------------------------------------------------------------------
    ((equity_contributed * base_pref_irr) * (year_index - 1))
      - LEAST(
          COALESCE(
            SUM(alloc_cash_investor) OVER (
              PARTITION BY portfolio_id, investor_id
              ORDER BY year
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
          ),
          (equity_contributed * base_pref_irr) * (year_index - 1)
        ) AS pref_balance_prior,

    --------------------------------------------------------------------
    -- (e) Pay Preferred THIS YEAR:
    --     = LEAST(pref_balance_prior, alloc_cash_investor)
    --------------------------------------------------------------------
    ROUND(
      CASE
        WHEN pref_balance_prior <= 0 THEN 0
        WHEN alloc_cash_investor >= pref_balance_prior THEN pref_balance_prior
        ELSE alloc_cash_investor
      END,
      2
    ) AS paid_pref

  FROM pref_calc AS pc
),

------------------------------------------------------------------------------
-- 7️⃣  ROC_CALC: After Preferred, pay Return of Capital (ROC)
------------------------------------------------------------------------------
roc_calc AS (
  SELECT
    ppc.*,

    --------------------------------------------------------------------
    -- (a) cash_after_pref_this_year = alloc_cash_investor - paid_pref
    --------------------------------------------------------------------
    (alloc_cash_investor - paid_pref) AS cash_after_pref_this_year,

    --------------------------------------------------------------------
    -- (b) cum_cash_available_prior  (from paid_pref_calc)
    --------------------------------------------------------------------
    ppc.cum_cash_available_prior,

    --------------------------------------------------------------------
    -- (c) cum_pref_paid_prior      (from paid_pref_calc)
    --------------------------------------------------------------------
    ppc.cum_pref_paid_prior,

    --------------------------------------------------------------------
    -- (d) Cumulative cash available for ROC PRIOR to this year:
    --     = cum_cash_available_prior - cum_pref_paid_prior
    --------------------------------------------------------------------
    (ppc.cum_cash_available_prior - ppc.cum_pref_paid_prior) AS cum_cash_for_roc_prior,

    --------------------------------------------------------------------
    -- (e) Cumulative ROC PAID PRIOR to this year:
    --     = LEAST(cum_cash_for_roc_prior, equity_contributed)
    --------------------------------------------------------------------
    LEAST(
      (ppc.cum_cash_available_prior - ppc.cum_pref_paid_prior),
      equity_contributed
    ) AS cum_roc_paid_prior,

    --------------------------------------------------------------------
    -- (f) Capital balance PRIOR to this year:
    --     = equity_contributed - cum_roc_paid_prior
    --------------------------------------------------------------------
    (equity_contributed
      - LEAST(
          (ppc.cum_cash_available_prior - ppc.cum_pref_paid_prior),
          equity_contributed
        )
    ) AS cap_balance_prior,

    --------------------------------------------------------------------
    -- (g) Pay ROC THIS YEAR:
    --     = LEAST(cap_balance_prior, cash_after_pref_this_year)
    --------------------------------------------------------------------
    ROUND(
      CASE
        WHEN cap_balance_prior <= 0 THEN 0
        WHEN cash_after_pref_this_year >= cap_balance_prior THEN cap_balance_prior
        ELSE cash_after_pref_this_year
      END,
      2
    ) AS paid_roc

  FROM paid_pref_calc AS ppc
),

------------------------------------------------------------------------------
-- 8️⃣  PROMOTE_POOL: After Preferred & ROC, any leftover cash goes to Promote Pool
------------------------------------------------------------------------------
promote_pool_calc AS (
  SELECT
    rc.*,

    --------------------------------------------------------------------
    -- Cash left after paying Pref & ROC this year:
    --     = GREATEST(alloc_cash_investor - paid_pref - paid_roc, 0)
    --------------------------------------------------------------------
    GREATEST((alloc_cash_investor - paid_pref - paid_roc), 0) AS promote_pool_this_year

  FROM roc_calc AS rc
),

------------------------------------------------------------------------------
-- 9️⃣  PROMOTE_SPLIT: Split the “promote pool” 70% to investors (pro-rata), 30% to sponsor
------------------------------------------------------------------------------
promote_split AS (
  SELECT
    ppc.*,

    --------------------------------------------------------------------
    -- Investor’s share of promote pool this year (pro-rata by class_ratio)
    --------------------------------------------------------------------
    ROUND(promote_pool_this_year * class_ratio, 2) AS investor_promote_this_year,

    --------------------------------------------------------------------
    -- Sponsor’s share of promote pool this year (30% of total)
    --------------------------------------------------------------------
    ROUND(promote_pool_this_year * 0.30, 2) AS sponsor_promote_this_year

  FROM promote_pool_calc AS ppc
),

------------------------------------------------------------------------------
-- 🔟  EXIT_SHORTFALL: In each investor’s FINAL ROW, top-up to hit exit IRR
------------------------------------------------------------------------------
exit_calc AS (
  SELECT
    ps.*,

    --------------------------------------------------------------------
    -- (a) Number of years this investor has been in the deal:
    --     = max(year_index) for that investor
    --------------------------------------------------------------------
    MAX(year_index) OVER (
      PARTITION BY portfolio_id, investor_id
    ) AS total_years_held,

    --------------------------------------------------------------------
    -- (b) Total $ needed to hit exit IRR by final year:
    --     = equity_contributed × ((1 + exit_irr)^total_years_held - 1)
    --------------------------------------------------------------------
    ROUND(
      equity_contributed
      * (
          POWER(
            1 + exit_irr,
            MAX(year_index) OVER ( PARTITION BY portfolio_id, investor_id )
          )
          - 1
        ),
      2
    ) AS total_exit_due,

    --------------------------------------------------------------------
    -- (c) Cumulative distributions paid PRIOR to this year
    --     (Pref + ROC + Promote)
    --------------------------------------------------------------------
    COALESCE(
      SUM(
        paid_pref
        + paid_roc
        + investor_promote_this_year
      ) OVER (
        PARTITION BY portfolio_id, investor_id
        ORDER BY year
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS cum_dist_paid_prior,

    --------------------------------------------------------------------
    -- (d) Cash still needed to hit exit target BEFORE this year:
    --     = total_exit_due - cum_dist_paid_prior
    --------------------------------------------------------------------
    GREATEST(
      (
        equity_contributed
        * (
            POWER(
              1 + exit_irr,
              MAX(year_index) OVER ( PARTITION BY portfolio_id, investor_id )
            )
            - 1
          )
      )
      - COALESCE(
          SUM(
            paid_pref
            + paid_roc
            + investor_promote_this_year
          ) OVER (
            PARTITION BY portfolio_id, investor_id
            ORDER BY year
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ),
          0
        ),
      0
    ) AS exit_balance_prior_inline,

    --------------------------------------------------------------------
    -- (e) Cash still available for exit top-up THIS YEAR:
    --     = GREATEST(alloc_cash_investor - paid_pref - paid_roc - investor_promote_this_year, 0)
    --------------------------------------------------------------------
    GREATEST(
      (alloc_cash_investor - paid_pref - paid_roc - investor_promote_this_year),
      0
    ) AS exit_cash_available_this_year_inline,

    --------------------------------------------------------------------
    -- (f) Pay exit shortfall ONLY in the investor’s FINAL ROW:
    --------------------------------------------------------------------
    CASE
      WHEN year_index = MAX(year_index) OVER (
                         PARTITION BY portfolio_id, investor_id
                       )
      THEN LEAST(
        -- inlined exit_balance_prior:
        GREATEST(
          (
            equity_contributed
            * (
                POWER(
                  1 + exit_irr,
                  MAX(year_index) OVER ( PARTITION BY portfolio_id, investor_id )
                )
                - 1
              )
          )
          - COALESCE(
              SUM(
                paid_pref
                + paid_roc
                + investor_promote_this_year
              ) OVER (
                PARTITION BY portfolio_id, investor_id
                ORDER BY year
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
              ),
              0
            ),
          0
        ),
        -- inlined exit_cash_available_this_year:
        GREATEST(
          (alloc_cash_investor - paid_pref - paid_roc - investor_promote_this_year),
          0
        )
      )
      ELSE 0
    END AS paid_exit_shortfall_this_year

  FROM promote_split AS ps
),

------------------------------------------------------------------------------
-- ⓫  FINAL_SPLIT: Collate investor-level payouts and label the cash tier
------------------------------------------------------------------------------
final_split AS (
  SELECT
    ec.portfolio_id,
    ec.year,
    ec.investor_id,
    ec.equity_class,
    ec.equity_contributed,
    ec.alloc_cash_investor,

    --------------------------------------------------------------------
    -- 1) Preferred paid this year
    --------------------------------------------------------------------
    ec.paid_pref             AS pref_paid,

    --------------------------------------------------------------------
    -- 2) ROC paid this year
    --------------------------------------------------------------------
    ec.paid_roc              AS roc_paid,

    --------------------------------------------------------------------
    -- 3) Investor’s Promote paid this year
    --------------------------------------------------------------------
    ec.investor_promote_this_year AS investor_promote,

    --------------------------------------------------------------------
    -- 4) Sponsor’s Promote paid this year (for reporting)
    --------------------------------------------------------------------
    ec.sponsor_promote_this_year  AS sponsor_promote,

    --------------------------------------------------------------------
    -- 5) Exit shortfall paid this year
    --------------------------------------------------------------------
    ec.paid_exit_shortfall_this_year AS exit_paid,

    --------------------------------------------------------------------
    -- 6) Label which “tier” this cash came from (debugging)
    --------------------------------------------------------------------
    CASE
      WHEN ec.paid_pref > 0                     THEN 'PREFERRED'
      WHEN ec.paid_roc  > 0                     THEN 'ROC'
      WHEN ec.investor_promote_this_year > 0    THEN 'PROMOTE'
      WHEN ec.paid_exit_shortfall_this_year > 0 THEN 'EXIT_SHORTFALL'
      ELSE 'UNALLOCATED'
    END                                    AS cash_tier

  FROM exit_calc AS ec
),

------------------------------------------------------------------------------
-- ⓬  AGGREGATE TO PORTFOLIO × YEAR: Compute distributable_profit
------------------------------------------------------------------------------
final_pool AS (
  SELECT
    fs.portfolio_id,
    fs.year,

    -- (1) Total ATCF flowing into portfolio that year
    SUM(fs.alloc_cash_investor)  AS total_atcf_portfolio,

    -- (2) Total Preferred paid to all investors that year
    SUM(fs.pref_paid)            AS total_pref_paid,

    -- (3) Total ROC paid to all investors that year
    SUM(fs.roc_paid)             AS total_roc_paid,

    -- (4) Total Promote paid to all investors that year
    SUM(fs.investor_promote)     AS total_promote_to_investors,

    -- (5) Total Promote paid to sponsor that year
    SUM(fs.sponsor_promote)      AS total_promote_to_sponsor,

    -- (6) Total Exit shortfall paid to all investors that year
    SUM(fs.exit_paid)            AS total_exit_paid,

    --------------------------------------------------------------------
    -- (7) Distributable Profit for that portfolio × year:
    --     = total_atcf_portfolio – (total_pref_paid + total_roc_paid + total_exit_paid)
    --------------------------------------------------------------------
    (
      SUM(fs.alloc_cash_investor)
      - SUM(fs.pref_paid)
      - SUM(fs.roc_paid)
      - SUM(fs.exit_paid)
    ) AS distributable_profit

  FROM final_split AS fs
  GROUP BY fs.portfolio_id, fs.year
),

------------------------------------------------------------------------------
-- ⓭  SUM ALL EQUITY per portfolio (Preferred + Common combined)
------------------------------------------------------------------------------
terms_summary AS (
  SELECT
    LOWER(portfolio_id)     AS portfolio_id,
    SUM(equity_contributed) AS total_equity
  FROM {{ source('hkh_dev', 'tbl_terms') }}
  GROUP BY LOWER(portfolio_id)
),

------------------------------------------------------------------------------
-- ⓮  CUMULATIVE MULTIPLE:
--      (cumulative ∑ distributable_profit up to & including this year) ÷ total_equity
------------------------------------------------------------------------------
cum_multiple_curr AS (
  SELECT
    fp.portfolio_id,
    fp.year,
    fp.distributable_profit,
    ts.total_equity,

    ------------------------------------------------------------------
    -- (a) Sum of distributable_profit from all prior years (< current)
    ------------------------------------------------------------------
    COALESCE(
      SUM(fp_prior.distributable_profit) OVER (
        PARTITION BY fp.portfolio_id
        ORDER BY fp_prior.year
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS cum_profit_prior,

    ------------------------------------------------------------------
    -- (b) Cumulative multiple at end of this year:
    --     = (cum_profit_prior + distributable_profit) / total_equity
    ------------------------------------------------------------------
    (
      COALESCE(
        SUM(fp_prior.distributable_profit) OVER (
          PARTITION BY fp.portfolio_id
          ORDER BY fp_prior.year
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        0
      )
      + fp.distributable_profit
    )::numeric 
      / NULLIF(ts.total_equity, 0) AS cum_multiple_current

  FROM final_pool AS fp
  LEFT JOIN final_pool AS fp_prior
    ON fp.portfolio_id = fp_prior.portfolio_id
      AND fp_prior.year < fp.year

  LEFT JOIN terms_summary AS ts
    ON fp.portfolio_id = ts.portfolio_id
),

------------------------------------------------------------------------------
-- ⓯  HURDLE_MATCH: Join each (portfolio, year) to every hurdle_tier where
--      cum_multiple_current ≤ irr_range_high, then rank by smallest irr_range_high
------------------------------------------------------------------------------
hurdle_match AS (
  SELECT
    cmc.portfolio_id,
    cmc.year,
    cmc.distributable_profit,
    cmc.cum_multiple_current,

    ht.hurdle_id,
    ht.irr_range_high,
    ht.common_share,
    ht.sponsor_share,

    ----------------------------------------------------------------
    -- Assign row_number so that “rn = 1” is the hurdle with the smallest
    -- irr_range_high that still covers our cumulative multiple
    ----------------------------------------------------------------
    ROW_NUMBER() OVER (
      PARTITION BY cmc.portfolio_id, cmc.year
      ORDER BY ht.irr_range_high ASC
    ) AS rn

  FROM cum_multiple_curr AS cmc
  JOIN {{ source('hkh_dev', 'tbl_hurdle_tiers') }} AS ht
    ON cmc.cum_multiple_current <= ht.irr_range_high
),

------------------------------------------------------------------------------
-- ⓰  HURDLE_SPLIT: Keep only the “first” matching hurdle (rn = 1)
------------------------------------------------------------------------------
hurdle_split AS (
  SELECT
    hm.portfolio_id,
    hm.year,
    hm.distributable_profit,
    hm.cum_multiple_current,

    hm.hurdle_id        AS matched_hurdle,
    hm.irr_range_high   AS matched_irr_high,
    hm.common_share     AS matched_common_share,
    hm.sponsor_share    AS matched_sponsor_share

  FROM hurdle_match AS hm
  WHERE hm.rn = 1
)

------------------------------------------------------------------------------
-- ⓱  FINAL OUTPUT: Multiply distributable_profit by matched shares, plus
--      all the placeholder bucket columns in one SELECT
------------------------------------------------------------------------------
SELECT
  hs.portfolio_id,
  hs.year,

  hs.distributable_profit,
  hs.cum_multiple_current,
  hs.matched_hurdle,
  hs.matched_irr_high,

  hs.matched_common_share,
  hs.matched_sponsor_share,

  /* 1) Investor-level buckets (placeholders → 0.00 for now): */
  0.00 AS pref_roc,       -- Placeholder: total pref principal returned
  0.00 AS pref_irr,       -- Placeholder: total pref IRR returned
  0.00 AS common_roc,     -- Placeholder: total common principal returned
  0.00 AS common_irr,     -- Placeholder: total common IRR returned

  /* 2) DISTRIBUTABLE PROFIT (again, just for reference) */
  hs.distributable_profit AS distributable_profit_again,

  /* 3) HURDLE-1 BUCKETS (placeholders) */
  0.00 AS hurdle1_total,
  0.00 AS hurdle1_common,
  0.00 AS hurdle1_sponsor,

  /* 4) HURDLE-2 BUCKETS (placeholders) */
  0.00 AS hurdle2_total,
  0.00 AS hurdle2_common,
  0.00 AS hurdle2_sponsor,

  /* 5) HURDLE-3 BUCKETS (placeholders) */
  0.00 AS hurdle3_total,
  0.00 AS hurdle3_common,
  0.00 AS hurdle3_sponsor,

  /* 6) RESIDUAL BUCKETS (beyond HURDLE-3) */
  0.00 AS residual_total,
  0.00 AS residual_common,
  0.00 AS residual_sponsor,

  /* 7) FINAL SPLIT of distributable_profit */
  ROUND(hs.distributable_profit * hs.matched_common_share, 2)  AS total_investor_share,
  ROUND(hs.distributable_profit * hs.matched_sponsor_share, 2) AS total_sponsor_share

FROM hurdle_split AS hs
ORDER BY hs.portfolio_id, hs.year