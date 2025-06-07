{{
  config(
    materialized='view'
  )
}}

WITH 
--------------------------------------------------------------------------------
-- First aggregate the terms data by portfolio
--------------------------------------------------------------------------------
portfolio_terms AS (
  SELECT
    portfolio_id,
    
    -- Aggregate by investor type
    SUM(CASE WHEN equity_class = 'Preferred' THEN equity_contributed ELSE 0 END) AS total_pref_equity,
    SUM(CASE WHEN equity_class = 'Common' THEN equity_contributed ELSE 0 END) AS total_common_equity,
    
    -- Weighted average base IRR for common investors
    CASE WHEN SUM(CASE WHEN equity_class = 'Common' THEN equity_contributed ELSE 0 END) > 0 THEN
      SUM(CASE WHEN equity_class = 'Common' THEN base_pref_irr * equity_contributed ELSE 0 END) / 
      SUM(CASE WHEN equity_class = 'Common' THEN equity_contributed ELSE 0 END)
    ELSE 0.07 END AS weighted_avg_base_irr
    
  FROM hkh_dev.tbl_terms
  GROUP BY portfolio_id
),

--------------------------------------------------------------------------------
-- Aggregate cash flows by portfolio and year first
--------------------------------------------------------------------------------
aggregated_cash_flows AS (
  SELECT
    cf.portfolio_id,
    cf.year,
    SUM(cf.atcf_operations) AS total_cash_flow
  FROM {{ ref('fact_property_cash_flow') }} cf
  GROUP BY cf.portfolio_id, cf.year
),

--------------------------------------------------------------------------------
-- Base cash flows with aggregated terms
--------------------------------------------------------------------------------
cash_flows_with_terms AS (
  SELECT
    acf.portfolio_id,
    acf.year,
    acf.total_cash_flow,
    
    -- Join aggregated terms data
    pt.total_pref_equity,
    pt.total_common_equity,
    pt.weighted_avg_base_irr,
    
    -- Cumulative cash flow by portfolio
    SUM(acf.total_cash_flow) OVER (
      PARTITION BY acf.portfolio_id 
      ORDER BY acf.year 
      ROWS UNBOUNDED PRECEDING
    ) AS cumulative_cash_flow
    
  FROM aggregated_cash_flows acf
  LEFT JOIN portfolio_terms pt ON acf.portfolio_id = pt.portfolio_id
),

--------------------------------------------------------------------------------
-- Calculate preferred capital outstanding
--------------------------------------------------------------------------------
pref_calculations AS (
  SELECT
    at.*,
    
    -- Preferred capital outstanding
    GREATEST(0, 
      at.total_pref_equity - COALESCE(
        SUM(LEAST(at.total_cash_flow, at.total_pref_equity)) OVER (
          PARTITION BY at.portfolio_id 
          ORDER BY at.year 
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0
      )
    ) AS pref_capital_outstanding,
    
    -- Is preferred ROC complete?
    CASE WHEN 
      GREATEST(0, 
        at.total_pref_equity - COALESCE(
          SUM(LEAST(at.total_cash_flow, at.total_pref_equity)) OVER (
            PARTITION BY at.portfolio_id 
            ORDER BY at.year 
            ROWS UNBOUNDED PRECEDING
          ), 0
        )
      ) = 0 
    THEN 1 ELSE 0 END AS pref_roc_complete
    
  FROM cash_flows_with_terms at
),

--------------------------------------------------------------------------------
-- Calculate preferred IRR accrual
--------------------------------------------------------------------------------
pref_irr_accrual AS (
  SELECT
    pc.*,
    
    -- Preferred IRR accrued (8% on outstanding preferred capital)
    pc.pref_capital_outstanding * 0.08 + 
    COALESCE(
      SUM(pc.pref_capital_outstanding * 0.08) OVER (
        PARTITION BY pc.portfolio_id 
        ORDER BY pc.year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0
    ) AS total_pref_irr_accrued
    
  FROM pref_calculations pc
),

--------------------------------------------------------------------------------
-- Calculate preferred payments
--------------------------------------------------------------------------------
waterfall_pref AS (
  SELECT
    pia.*,
    
    -- Preferred ROC payment
    LEAST(pia.total_cash_flow, pia.pref_capital_outstanding) AS pref_roc_paid,
    
    -- Preferred IRR payment (only when ROC is fully paid back)
    CASE 
      WHEN pia.pref_roc_complete = 1 THEN
        GREATEST(0, LEAST(
          pia.total_cash_flow - LEAST(pia.total_cash_flow, pia.pref_capital_outstanding),
          pia.total_pref_irr_accrued
        ))
      ELSE 0
    END AS pref_irr_paid,
    
    -- Cash remaining for common after preferred payments
    pia.total_cash_flow - LEAST(pia.total_cash_flow, pia.pref_capital_outstanding) - 
    CASE 
      WHEN pia.pref_roc_complete = 1 THEN
        GREATEST(0, LEAST(
          pia.total_cash_flow - LEAST(pia.total_cash_flow, pia.pref_capital_outstanding),
          pia.total_pref_irr_accrued
        ))
      ELSE 0
    END AS cash_for_common
    
  FROM pref_irr_accrual pia
),

--------------------------------------------------------------------------------
-- Track IRR payments to prevent double-paying
--------------------------------------------------------------------------------
waterfall_pref_final AS (
  SELECT
    wp.*,
    
    -- Adjusted IRR payment (don't pay more than what's owed)
    GREATEST(0, LEAST(
      wp.pref_irr_paid,
      wp.total_pref_irr_accrued - COALESCE(
        SUM(wp.pref_irr_paid) OVER (
          PARTITION BY wp.portfolio_id 
          ORDER BY wp.year 
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0
      )
    )) AS pref_irr_paid_adjusted,
    
    -- Recalculate cash for common with adjusted IRR
    wp.total_cash_flow - wp.pref_roc_paid - GREATEST(0, LEAST(
      wp.pref_irr_paid,
      wp.total_pref_irr_accrued - COALESCE(
        SUM(wp.pref_irr_paid) OVER (
          PARTITION BY wp.portfolio_id 
          ORDER BY wp.year 
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0
      )
    )) AS cash_for_common_adjusted
    
  FROM waterfall_pref wp
),

--------------------------------------------------------------------------------
-- Calculate common payments with IRR gating
--------------------------------------------------------------------------------
waterfall_common AS (
  SELECT
    wpf.*,
    
    -- Common capital outstanding
    GREATEST(0, 
      wpf.total_common_equity - COALESCE(
        SUM(
          CASE WHEN pref_capital_outstanding = 0 THEN
            LEAST(cash_for_common_adjusted, total_common_equity)
          ELSE 0 END
        ) OVER (
          PARTITION BY wpf.portfolio_id 
          ORDER BY wpf.year 
          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ), 0
      )
    ) AS common_capital_outstanding,
    
    -- Common IRR accrued at weighted average base IRR rate
    CASE 
      WHEN wpf.pref_capital_outstanding = 0 THEN
        wpf.weighted_avg_base_irr * wpf.year * wpf.total_common_equity * 0.1
      ELSE 0
    END AS total_common_irr_accrued,
    
    -- Common ROC payment (only when preferred is completely done)
    CASE 
      WHEN wpf.pref_capital_outstanding = 0 THEN
        GREATEST(0, LEAST(
          wpf.cash_for_common_adjusted,
          GREATEST(0, 
            wpf.total_common_equity - COALESCE(
              SUM(
                CASE WHEN pref_capital_outstanding = 0 THEN
                  LEAST(cash_for_common_adjusted, total_common_equity)
                ELSE 0 END
              ) OVER (
                PARTITION BY wpf.portfolio_id 
                ORDER BY wpf.year 
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
              ), 0
            )
          )
        ))
      ELSE 0
    END AS common_roc_paid,
    
    -- Is common ROC complete?
    CASE WHEN wpf.pref_capital_outstanding = 0 AND
      GREATEST(0, 
        wpf.total_common_equity - COALESCE(
          SUM(
            CASE WHEN pref_capital_outstanding = 0 THEN
              LEAST(cash_for_common_adjusted, total_common_equity)
            ELSE 0 END
          ) OVER (
            PARTITION BY wpf.portfolio_id 
            ORDER BY wpf.year 
            ROWS UNBOUNDED PRECEDING
          ), 0
        )
      ) = 0
    THEN 1 ELSE 0 END AS common_roc_complete
    
  FROM waterfall_pref_final wpf
),

--------------------------------------------------------------------------------
-- Calculate common IRR payments and remaining cash for hurdles
--------------------------------------------------------------------------------
waterfall_common_irr AS (
  SELECT
    wc.*,
    
    -- Common IRR payment (only after common ROC is complete)
    CASE 
      WHEN wc.common_roc_complete = 1 THEN
        GREATEST(0, LEAST(
          wc.cash_for_common_adjusted - wc.common_roc_paid,
          wc.total_common_irr_accrued - COALESCE(
            SUM(
              CASE WHEN common_roc_complete = 1 THEN
                GREATEST(0, LEAST(
                  cash_for_common_adjusted - common_roc_paid,
                  total_common_irr_accrued
                ))
              ELSE 0 END
            ) OVER (
              PARTITION BY wc.portfolio_id 
              ORDER BY wc.year 
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0
          )
        ))
      ELSE 0
    END AS common_irr_paid,
    
    -- Cash remaining for hurdles after common ROC and IRR
    wc.cash_for_common_adjusted - wc.common_roc_paid - 
    CASE 
      WHEN wc.common_roc_complete = 1 THEN
        GREATEST(0, LEAST(
          wc.cash_for_common_adjusted - wc.common_roc_paid,
          wc.total_common_irr_accrued - COALESCE(
            SUM(
              CASE WHEN common_roc_complete = 1 THEN
                GREATEST(0, LEAST(
                  cash_for_common_adjusted - common_roc_paid,
                  total_common_irr_accrued
                ))
              ELSE 0 END
            ) OVER (
              PARTITION BY wc.portfolio_id 
              ORDER BY wc.year 
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0
          )
        ))
      ELSE 0
    END AS cash_for_hurdles
    
  FROM waterfall_common wc
),

--------------------------------------------------------------------------------
-- Calculate hurdle payments with proper gating
--------------------------------------------------------------------------------
waterfall_hurdles AS (
  SELECT
    wci.*,
    
    -- Hurdle 1: Gap from weighted avg base IRR to 8% (only when common IRR target met)
    CASE WHEN wci.common_roc_complete = 1 AND wci.total_common_irr_accrued > 0 THEN
      GREATEST(0, wci.cash_for_hurdles * 0.2 * LEAST(1.0, (0.08 - wci.weighted_avg_base_irr) / 0.01))
    ELSE 0 END AS hurdle_1_common_paid,
    
    CASE WHEN wci.common_roc_complete = 1 AND wci.total_common_irr_accrued > 0 THEN
      GREATEST(0, wci.cash_for_hurdles * 0.8 * LEAST(1.0, (0.08 - wci.weighted_avg_base_irr) / 0.01))
    ELSE 0 END AS hurdle_1_sponsor_paid,
    
    -- Hurdle 2: Everything above the hurdle 1 threshold (20/80 split)
    CASE WHEN wci.common_roc_complete = 1 AND wci.total_common_irr_accrued > 0 THEN
      GREATEST(0, wci.cash_for_hurdles * 0.2 * (1.0 - LEAST(1.0, (0.08 - wci.weighted_avg_base_irr) / 0.01)))
    ELSE 0 END AS hurdle_2_common_paid,
    
    CASE WHEN wci.common_roc_complete = 1 AND wci.total_common_irr_accrued > 0 THEN
      GREATEST(0, wci.cash_for_hurdles * 0.8 * (1.0 - LEAST(1.0, (0.08 - wci.weighted_avg_base_irr) / 0.01)))
    ELSE 0 END AS hurdle_2_sponsor_paid
    
  FROM waterfall_common_irr wci
)

--------------------------------------------------------------------------------
-- Final output aggregated by portfolio
--------------------------------------------------------------------------------
SELECT
  portfolio_id,
  year,
  ROUND(total_cash_flow, 2) AS total_cash_flow,
  
  -- Preferred distributions
  ROUND(pref_roc_paid, 2) AS pref_roc_paid,
  ROUND(pref_irr_paid_adjusted, 2) AS pref_irr_paid,
  ROUND(pref_roc_paid + pref_irr_paid_adjusted, 2) AS pref_total,
  
  -- Common distributions  
  ROUND(common_roc_paid, 2) AS common_roc_paid,
  ROUND(common_irr_paid, 2) AS common_irr_paid,
  ROUND(hurdle_1_common_paid + hurdle_2_common_paid, 2) AS hurdle_common_paid,
  
  -- Sponsor distributions
  ROUND(hurdle_1_sponsor_paid + hurdle_2_sponsor_paid, 2) AS hurdle_sponsor_paid,
  
  -- Party totals
  ROUND(common_roc_paid + common_irr_paid + hurdle_1_common_paid + hurdle_2_common_paid, 2) AS common_total,
  ROUND(hurdle_1_sponsor_paid + hurdle_2_sponsor_paid, 2) AS sponsor_total,
  
  -- Validation
  ROUND(pref_roc_paid + pref_irr_paid_adjusted + common_roc_paid + common_irr_paid + 
        hurdle_1_common_paid + hurdle_2_common_paid + hurdle_1_sponsor_paid + hurdle_2_sponsor_paid, 2) AS total_distributed,
  
  -- Debug info
  ROUND(pref_capital_outstanding, 2) AS pref_capital_outstanding,
  ROUND(total_pref_irr_accrued, 2) AS total_pref_irr_accrued,
  ROUND(common_capital_outstanding, 2) AS common_capital_outstanding,
  ROUND(total_common_irr_accrued, 2) AS total_common_irr_accrued,
  ROUND(total_common_equity, 2) AS common_total_equity,
  ROUND(weighted_avg_base_irr, 4) AS weighted_avg_base_irr,
  common_roc_complete,
  ROUND(cumulative_cash_flow, 2) AS cumulative_cash_flow
  
FROM waterfall_hurdles
ORDER BY portfolio_id, year