{{ config(materialized='view') }}
-- ----------------------------------------------------------------------------------
-- models/debug_pref_flow.sql
--
-- Only “Preferred class” IRR→ROC, year by year.  Designed to be as tiny as possible:
--   •  Filter tbl_terms to equity_class = 'Preferred'
--   •  Aggregate ATCF by portfolio × year, allocate pro‐rata across Pref investors
--   •  In each year for each investor:  
--       – Compute prior balance = original equity − cumulative principal paid so far  
--       – Compute interest_due = (prior balance) × base_pref_irr  
--       – Pay interest out of that year’s alloc_cash (up to interest_due)  
--       – Pay principal out of whatever cash remains (up to the prior balance)  
--   •  Output year-by-year figures so you can verify on paper exactly how it should work.
-- ----------------------------------------------------------------------------------

WITH
--------------------------------------------------------------------------------
-- 1️⃣ TERMS_PREF:  Only Preferred investors
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
-- 3️⃣ INVESTOR_SHARES_PREF:  Allocate portfolio cash to Pref investors (pro-rata)
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

    -- Pro-rata among only “Preferred” investors in this portfolio
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
-- 4️⃣ ALLOC_PREF:  Compute each Pref investor’s absolute cash for the year
--------------------------------------------------------------------------------
alloc_pref AS (
  SELECT
    *,
    ROUND(alloc_cash_portfolio * pref_class_ratio, 2) AS alloc_cash_investor
  FROM investor_shares_pref
),

--------------------------------------------------------------------------------
-- 5️⃣ PREF_FLOW:  For each Pref investor × year, pay:
--   (a) any accrued interest on prior‐year balance (balance_prior * base_pref_irr),
--   (b) then as much principal as remains (up to that prior balance).
--
-- We use window‐functions to carry “cum_pref_principal_paid_prior” forward.
--------------------------------------------------------------------------------
pref_flow AS (
  SELECT
    ap.portfolio_id,
    ap.year,
    ap.investor_id,
    ap.equity_contributed,
    ap.base_pref_irr,
    ap.alloc_cash_investor,

    ------------------------------------------------------------------------
    -- (a) Cumulative Pref principal paid PRIOR to this year:
    ------------------------------------------------------------------------
    COALESCE(
      SUM(
        CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
      ) OVER (
        PARTITION BY ap.portfolio_id, ap.investor_id
        ORDER BY ap.year
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ),
      0
    ) AS cum_pref_principal_paid_prior,

    ------------------------------------------------------------------------
    -- (b) Pref capital BALANCE PRIOR to this year:
    --     = original equity_contributed – cum_pref_principal_paid_prior
    ------------------------------------------------------------------------
    (ap.equity_contributed
     - COALESCE(
         SUM(
           CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
         ) OVER (
           PARTITION BY ap.portfolio_id, ap.investor_id
           ORDER BY ap.year
           ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
         ),
         0
       )
    ) AS pref_cap_balance_prior,

    ------------------------------------------------------------------------
    -- (c) Pref interest DUE this year:
    --     = pref_cap_balance_prior * base_pref_irr
    ------------------------------------------------------------------------
    ROUND(
      (
        (ap.equity_contributed
         - COALESCE(
             SUM(
               CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
             ) OVER (
               PARTITION BY ap.portfolio_id, ap.investor_id
               ORDER BY ap.year
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
             ),
             0
           )
        ) * ap.base_pref_irr
      ),
      2
    ) AS pref_interest_due,

    ------------------------------------------------------------------------
    -- (d) Cash AVAILABLE THIS YEAR for this investor:
    --     = cum_prior_alloc + alloc_cash_investor
    ------------------------------------------------------------------------
    (
      COALESCE(
        SUM(ap.alloc_cash_investor) OVER (
          PARTITION BY ap.portfolio_id, ap.investor_id
          ORDER BY ap.year
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ),
        0
      )
      + ap.alloc_cash_investor
    ) AS cash_available_this_year,

    ------------------------------------------------------------------------
    -- (e) Pref interest PAID this year:
    --     = LEAST(pref_interest_due, cash_available_this_year)
    ------------------------------------------------------------------------
    ROUND(
      LEAST(
        (ap.equity_contributed
         - COALESCE(
             SUM(
               CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
             ) OVER (
               PARTITION BY ap.portfolio_id, ap.investor_id
               ORDER BY ap.year
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
             ),
             0
           )
        ) * ap.base_pref_irr,
        (
          COALESCE(
            SUM(ap.alloc_cash_investor) OVER (
              PARTITION BY ap.portfolio_id, ap.investor_id
              ORDER BY ap.year
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
          )
          + ap.alloc_cash_investor
        )
      ),
      2
    ) AS pref_interest_paid,

    ------------------------------------------------------------------------
    -- (f) Cash REMAINING after paying Pref interest:
    --     = cash_available_this_year − pref_interest_paid
    ------------------------------------------------------------------------
    (
      ( COALESCE(
          SUM(ap.alloc_cash_investor) OVER (
            PARTITION BY ap.portfolio_id, ap.investor_id
            ORDER BY ap.year
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
          ), 
          0
        )
        + ap.alloc_cash_investor
      )
      - ROUND(
          LEAST(
            (
              (ap.equity_contributed
               - COALESCE(
                   SUM(
                     CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
                   ) OVER (
                     PARTITION BY ap.portfolio_id, ap.investor_id
                     ORDER BY ap.year
                     ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                   ),
                   0
                 )
              ) * ap.base_pref_irr
            ),
            (
              COALESCE(
                SUM(ap.alloc_cash_investor) OVER (
                  PARTITION BY ap.portfolio_id, ap.investor_id
                  ORDER BY ap.year
                  ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ),
                0
              )
              + ap.alloc_cash_investor
            )
          ),
          2
        )
    ) AS cash_after_pref_interest,

    ------------------------------------------------------------------------
    -- (g) Pref principal (ROC) PAID this year:
    --     = LEAST(pref_cap_balance_prior, cash_after_pref_interest)
    ------------------------------------------------------------------------
    ROUND(
      LEAST(
        ( ap.equity_contributed
          - COALESCE(
              SUM(
                CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
              ) OVER (
                PARTITION BY ap.portfolio_id, ap.investor_id
                ORDER BY ap.year
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
              ),
              0
            )
        ),
        (
          ( COALESCE(
              SUM(ap.alloc_cash_investor) OVER (
                PARTITION BY ap.portfolio_id, ap.investor_id
                ORDER BY ap.year
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
              ),
              0
            )
            + ap.alloc_cash_investor
          )
          - ROUND(
              LEAST(
                (
                  (ap.equity_contributed
                   - COALESCE(
                       SUM(
                         CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
                       ) OVER (
                         PARTITION BY ap.portfolio_id, ap.investor_id
                         ORDER BY ap.year
                         ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                       ),
                       0
                     )
                  ) * ap.base_pref_irr
                ),
                (
                  COALESCE(
                    SUM(ap.alloc_cash_investor) OVER (
                      PARTITION BY ap.portfolio_id, ap.investor_id
                      ORDER BY ap.year
                      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                    ),
                    0
                  )
                  + ap.alloc_cash_investor
                )
              ),
              2
            )
        )
      ),
      2
    ) AS pref_principal_paid,

    ------------------------------------------------------------------------
    -- (h) For debugging: end‐of‐year Pref balance that flows into next year:
    --     = pref_cap_balance_prior − pref_principal_paid
    ------------------------------------------------------------------------
    ROUND(
      (
        ( ap.equity_contributed
          - COALESCE(
              SUM(
                CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
              ) OVER (
                PARTITION BY ap.portfolio_id, ap.investor_id
                ORDER BY ap.year
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
              ),
              0
            )
        )
        - LEAST(
            ( ap.equity_contributed
              - COALESCE(
                  SUM(
                    CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
                  ) OVER (
                    PARTITION BY ap.portfolio_id, ap.investor_id
                    ORDER BY ap.year
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                  ),
                  0
                )
            ),
            (
              ( COALESCE(
                  SUM(ap.alloc_cash_investor) OVER (
                    PARTITION BY ap.portfolio_id, ap.investor_id
                    ORDER BY ap.year
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                  ),
                  0
                )
                + ap.alloc_cash_investor
              )
              - ROUND(
                  LEAST(
                    (
                      (ap.equity_contributed
                       - COALESCE(
                           SUM(
                             CASE WHEN equity_class = 'Preferred' THEN roc_paid ELSE 0 END
                           ) OVER (
                             PARTITION BY ap.portfolio_id, ap.investor_id
                             ORDER BY ap.year
                             ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                           ),
                           0
                         )
                      ) * ap.base_pref_irr
                    ),
                    (
                      COALESCE(
                        SUM(ap.alloc_cash_investor) OVER (
                          PARTITION BY ap.portfolio_id, ap.investor_id
                          ORDER BY ap.year
                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                        ),
                        0
                      )
                      + ap.alloc_cash_investor
                    )
                  ),
                  2
                )
            )
          )
      ),
      2
    ) AS pref_cap_balance_end

  FROM alloc_pref AS ap
  WHERE ap.equity_class = 'Preferred'
)

SELECT
  *
FROM pref_flow
ORDER BY portfolio_id, investor_id, year