{{ config(materialized='view') }}
-- ----------------------------------------------------------------------------------
-- Complete Real Estate Waterfall Model - WORKING VERSION
-- Handles: Preferred ROC/IRR → Common ROC/IRR → Hurdle Tiers → Residual
-- ----------------------------------------------------------------------------------

WITH
--------------------------------------------------------------------------------
-- Equity classes and their weighted average IRRs
--------------------------------------------------------------------------------
equity_classes AS (
  SELECT
    LOWER(portfolio_id) AS portfolio_id,
    equity_class,
    SUM(equity_contributed * base_pref_irr) / SUM(equity_contributed) AS weighted_avg_irr,
    SUM(equity_contributed) AS total_equity
  FROM "hkh_decision_support_db"."hkh_dev"."tbl_terms"
  WHERE equity_class IN ('Preferred', 'Common')
  GROUP BY LOWER(portfolio_id), equity_class
),

--------------------------------------------------------------------------------
-- Hurdle structure
--------------------------------------------------------------------------------
hurdles AS (
  SELECT
    hurdle_id,
    irr_range_high AS hurdle_irr,
    common_share,
    sponsor_share,
    CASE 
      WHEN hurdle_id = 'hurdle1' THEN 1
      WHEN hurdle_id = 'hurdle2' THEN 2  
      WHEN hurdle_id = 'hurdle3' THEN 3
      WHEN hurdle_id = 'residual' THEN 4
    END AS tier_order,
    LAG(irr_range_high, 1, 0) OVER (ORDER BY 
      CASE 
        WHEN hurdle_id = 'hurdle1' THEN 1
        WHEN hurdle_id = 'hurdle2' THEN 2
        WHEN hurdle_id = 'hurdle3' THEN 3  
        WHEN hurdle_id = 'residual' THEN 4
      END
    ) AS prior_hurdle_irr
  FROM "hkh_decision_support_db"."hkh"."tbl_hurdle_tiers"
),

--------------------------------------------------------------------------------
-- Portfolio cash by year
--------------------------------------------------------------------------------
cash_flows AS (
  SELECT
    LOWER(pi.portfolio_id) AS portfolio_id,
    fpf.year,
    SUM(fpf.atcf) AS annual_cash_flow,
    SUM(SUM(fpf.atcf)) OVER (
      PARTITION BY LOWER(pi.portfolio_id) 
      ORDER BY fpf.year 
      ROWS UNBOUNDED PRECEDING
    ) AS cumulative_cash_flow
  FROM "hkh_decision_support_db"."hkh_dev"."fact_property_cash_flow" AS fpf
  JOIN "hkh_decision_support_db"."inputs"."property_inputs" AS pi
    ON fpf.property_id = pi.property_id
  GROUP BY LOWER(pi.portfolio_id), fpf.year
),

--------------------------------------------------------------------------------
-- Main waterfall calculation
--------------------------------------------------------------------------------
waterfall_base AS (
  SELECT
    cf.portfolio_id,
    cf.year,
    cf.annual_cash_flow,
    cf.cumulative_cash_flow,
    
    -- Equity amounts
    COALESCE(pref.total_equity, 0) AS pref_equity,
    COALESCE(common.total_equity, 0) AS common_equity,
    COALESCE(pref.total_equity, 0) + COALESCE(common.total_equity, 0) AS total_equity,
    
    -- IRR targets
    COALESCE(pref.weighted_avg_irr, 0) AS pref_target_irr,
    COALESCE(common.weighted_avg_irr, 0) AS common_target_irr,
    
    -- Current portfolio IRR (simplified as cumulative cash / total equity)
    CASE 
      WHEN COALESCE(pref.total_equity, 0) + COALESCE(common.total_equity, 0) > 0
      THEN cf.cumulative_cash_flow / (COALESCE(pref.total_equity, 0) + COALESCE(common.total_equity, 0))
      ELSE 0 
    END AS current_portfolio_multiple

  FROM cash_flows cf
  LEFT JOIN equity_classes pref 
    ON cf.portfolio_id = pref.portfolio_id AND pref.equity_class = 'Preferred'
  LEFT JOIN equity_classes common 
    ON cf.portfolio_id = common.portfolio_id AND common.equity_class = 'Common'
),

--------------------------------------------------------------------------------
-- Calculate cumulative distributions needed for each class
--------------------------------------------------------------------------------
distributions_needed AS (
  SELECT
    *,
    -- Preferred total return needed (ROC + IRR)
    pref_equity * (1 + pref_target_irr) AS pref_total_needed,
    
    -- Common total return needed (ROC + IRR) 
    common_equity * (1 + common_target_irr) AS common_total_needed,
    
    -- Cumulative preferred distributions needed through this year
    SUM(pref_equity * (1 + pref_target_irr)) OVER (
      PARTITION BY portfolio_id 
      ORDER BY year 
      ROWS UNBOUNDED PRECEDING
    ) AS cum_pref_needed,
    
    -- Cumulative common distributions needed through this year
    SUM(common_equity * (1 + common_target_irr)) OVER (
      PARTITION BY portfolio_id 
      ORDER BY year 
      ROWS UNBOUNDED PRECEDING  
    ) AS cum_common_needed

  FROM waterfall_base
),

--------------------------------------------------------------------------------
-- Final waterfall distribution
--------------------------------------------------------------------------------
final_waterfall AS (
  SELECT
    portfolio_id,
    year,
    annual_cash_flow,
    
    -- Step 1: Pay Preferred first
    CASE 
      WHEN cumulative_cash_flow <= cum_pref_needed 
      THEN annual_cash_flow
      WHEN cumulative_cash_flow - annual_cash_flow < cum_pref_needed
      THEN cum_pref_needed - (cumulative_cash_flow - annual_cash_flow)
      ELSE 0
    END AS pref_distribution,
    
    -- Step 2: Pay Common second  
    CASE
      WHEN cumulative_cash_flow <= cum_pref_needed THEN 0
      WHEN cumulative_cash_flow <= cum_pref_needed + cum_common_needed
      THEN LEAST(
        annual_cash_flow - CASE 
          WHEN cumulative_cash_flow <= cum_pref_needed THEN annual_cash_flow
          WHEN cumulative_cash_flow - annual_cash_flow < cum_pref_needed
          THEN cum_pref_needed - (cumulative_cash_flow - annual_cash_flow)
          ELSE 0
        END,
        (cum_pref_needed + cum_common_needed) - (cumulative_cash_flow - annual_cash_flow)
      )
      ELSE common_equity * (1 + common_target_irr)
    END AS common_distribution,
    
    -- Step 3: Remaining cash for promote/hurdles
    GREATEST(0, 
      annual_cash_flow 
      - CASE 
          WHEN cumulative_cash_flow <= cum_pref_needed 
          THEN annual_cash_flow
          WHEN cumulative_cash_flow - annual_cash_flow < cum_pref_needed
          THEN cum_pref_needed - (cumulative_cash_flow - annual_cash_flow)
          ELSE 0
        END
      - CASE
          WHEN cumulative_cash_flow <= cum_pref_needed THEN 0
          WHEN cumulative_cash_flow <= cum_pref_needed + cum_common_needed
          THEN LEAST(
            annual_cash_flow - CASE 
              WHEN cumulative_cash_flow <= cum_pref_needed THEN annual_cash_flow
              WHEN cumulative_cash_flow - annual_cash_flow < cum_pref_needed 
              THEN cum_pref_needed - (cumulative_cash_flow - annual_cash_flow)
              ELSE 0
            END,
            (cum_pref_needed + cum_common_needed) - (cumulative_cash_flow - annual_cash_flow)
          )
          ELSE common_equity * (1 + common_target_irr)
        END
    ) AS promote_pool,
    
    current_portfolio_multiple,
    pref_equity,
    common_equity,
    total_equity

  FROM distributions_needed
),

--------------------------------------------------------------------------------
-- Apply hurdle tier splits to promote pool
--------------------------------------------------------------------------------
hurdle_splits AS (
  SELECT
    fw.*,
    
    -- Hurdle 1 (up to 8%)
    CASE 
      WHEN current_portfolio_multiple <= 0.08 THEN promote_pool * 0.7
      WHEN current_portfolio_multiple > 0.08 THEN promote_pool * 0.08 / current_portfolio_multiple * 0.7
      ELSE 0
    END AS h1_common,
    
    CASE 
      WHEN current_portfolio_multiple <= 0.08 THEN promote_pool * 0.3
      WHEN current_portfolio_multiple > 0.08 THEN promote_pool * 0.08 / current_portfolio_multiple * 0.3  
      ELSE 0
    END AS h1_sponsor,
    
    -- Hurdle 2 (8% to 12%)
    CASE
      WHEN current_portfolio_multiple <= 0.08 THEN 0
      WHEN current_portfolio_multiple <= 0.12 THEN promote_pool * ((current_portfolio_multiple - 0.08) / current_portfolio_multiple) * 0.6
      WHEN current_portfolio_multiple > 0.12 THEN promote_pool * (0.04 / current_portfolio_multiple) * 0.6
      ELSE 0  
    END AS h2_common,
    
    CASE
      WHEN current_portfolio_multiple <= 0.08 THEN 0
      WHEN current_portfolio_multiple <= 0.12 THEN promote_pool * ((current_portfolio_multiple - 0.08) / current_portfolio_multiple) * 0.4
      WHEN current_portfolio_multiple > 0.12 THEN promote_pool * (0.04 / current_portfolio_multiple) * 0.4
      ELSE 0
    END AS h2_sponsor,
    
    -- Hurdle 3 (12% to 18%)  
    CASE
      WHEN current_portfolio_multiple <= 0.12 THEN 0
      WHEN current_portfolio_multiple <= 0.18 THEN promote_pool * ((current_portfolio_multiple - 0.12) / current_portfolio_multiple) * 0.5
      WHEN current_portfolio_multiple > 0.18 THEN promote_pool * (0.06 / current_portfolio_multiple) * 0.5
      ELSE 0
    END AS h3_common,
    
    CASE  
      WHEN current_portfolio_multiple <= 0.12 THEN 0
      WHEN current_portfolio_multiple <= 0.18 THEN promote_pool * ((current_portfolio_multiple - 0.12) / current_portfolio_multiple) * 0.5
      WHEN current_portfolio_multiple > 0.18 THEN promote_pool * (0.06 / current_portfolio_multiple) * 0.5
      ELSE 0
    END AS h3_sponsor,
    
    -- Residual (above 18%)
    CASE
      WHEN current_portfolio_multiple <= 0.18 THEN 0  
      ELSE promote_pool * ((current_portfolio_multiple - 0.18) / current_portfolio_multiple) * 0.35
    END AS residual_common,
    
    CASE
      WHEN current_portfolio_multiple <= 0.18 THEN 0
      ELSE promote_pool * ((current_portfolio_multiple - 0.18) / current_portfolio_multiple) * 0.65  
    END AS residual_sponsor

  FROM final_waterfall fw
)

SELECT
  portfolio_id,
  year,
  annual_cash_flow,
  
  -- Core distributions
  ROUND(pref_distribution, 2) AS pref_total,
  ROUND(common_distribution, 2) AS common_total,
  
  -- Hurdle distributions
  ROUND(h1_common, 2) AS h1_common,
  ROUND(h1_sponsor, 2) AS h1_sponsor, 
  ROUND(h2_common, 2) AS h2_common,
  ROUND(h2_sponsor, 2) AS h2_sponsor,
  ROUND(h3_common, 2) AS h3_common,
  ROUND(h3_sponsor, 2) AS h3_sponsor,
  ROUND(residual_common, 2) AS residual_common,
  ROUND(residual_sponsor, 2) AS residual_sponsor,
  
  -- Totals for verification
  ROUND(pref_distribution + common_distribution + h1_common + h1_sponsor + 
        h2_common + h2_sponsor + h3_common + h3_sponsor + 
        residual_common + residual_sponsor, 2) AS total_distributed,
  
  -- Portfolio metrics
  ROUND(current_portfolio_multiple, 4) AS portfolio_multiple,
  pref_equity,
  common_equity,
  total_equity

FROM hurdle_splits
ORDER BY portfolio_id, year