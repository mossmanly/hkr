{{ config(materialized='view') }}
-- ----------------------------------------------------------------------------------
-- Complete Real Estate Waterfall Model
-- 
-- Processes cash flows through the full waterfall structure:
-- 1. Preferred ROC + IRR
-- 2. Common ROC + IRR  
-- 3. Hurdle tiers with promote splits
-- 4. Residual splits
--
-- Outputs clean buckets for distribution to individual investors
-- ----------------------------------------------------------------------------------

WITH
--------------------------------------------------------------------------------
-- FOUNDATION: Get investor terms and cash flows
--------------------------------------------------------------------------------
investor_terms AS (
  SELECT
    LOWER(portfolio_id) AS portfolio_id,
    equity_class,
    SUM(equity_contributed) AS total_equity_by_class,
    -- Weighted average IRR by class
    SUM(equity_contributed * base_pref_irr) / SUM(equity_contributed) AS weighted_avg_irr
  FROM {{ ref('hkh_dev', 'tbl_terms') }}
  WHERE equity_class IN ('Preferred', 'Common')
  GROUP BY LOWER(portfolio_id), equity_class
),

portfolio_cash AS (
  SELECT
    LOWER(pi.portfolio_id) AS portfolio_id,
    fpf.year,
    SUM(fpf.atcf) AS total_cash_flow
  FROM {{ ref('hkh_dev','fact_property_cash_flow') }} AS fpf
  JOIN {{ source('inputs', 'property_inputs') }} AS pi
    ON fpf.property_id = pi.property_id
  GROUP BY LOWER(pi.portfolio_id), fpf.year
),

hurdle_tiers AS (
  SELECT
    hurdle_id,
    irr_range_high,
    common_share,
    sponsor_share
  FROM {{ ref('hkh_dev','tbl_hurdle_tiers') }}
),

--------------------------------------------------------------------------------
-- STEP 1: Calculate cumulative returns and remaining balances
--------------------------------------------------------------------------------
investor_balances AS (
  SELECT
    pc.portfolio_id,
    pc.year,
    pc.total_cash_flow,
    
    -- Preferred class totals
    COALESCE(pref.total_equity_by_class, 0) AS pref_total_equity,
    COALESCE(pref.weighted_avg_irr, 0) AS pref_weighted_irr,
    
    -- Common class totals  
    COALESCE(comm.total_equity_by_class, 0) AS common_total_equity,
    COALESCE(comm.weighted_avg_irr, 0) AS common_weighted_irr,
    
    -- Total equity for IRR calculations
    COALESCE(pref.total_equity_by_class, 0) + COALESCE(comm.total_equity_by_class, 0) AS total_equity,
    
    -- Cumulative cash distributed so far (for IRR calculations)
    SUM(pc.total_cash_flow) OVER (
      PARTITION BY pc.portfolio_id 
      ORDER BY pc.year 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_cash_distributed,
    
    -- Cumulative cash available for this year
    SUM(pc.total_cash_flow) OVER (
      PARTITION BY pc.portfolio_id 
      ORDER BY pc.year 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cash_available
    
  FROM portfolio_cash AS pc
  LEFT JOIN investor_terms AS pref 
    ON pc.portfolio_id = pref.portfolio_id AND pref.equity_class = 'Preferred'
  LEFT JOIN investor_terms AS comm 
    ON pc.portfolio_id = comm.portfolio_id AND comm.equity_class = 'Common'
),

--------------------------------------------------------------------------------
-- STEP 2: Calculate what each tier is owed (cumulative targets)
--------------------------------------------------------------------------------
tier_targets AS (
  SELECT
    *,
    
    -- Preferred targets (ROC + IRR)
    pref_total_equity AS pref_roc_target,
    pref_total_equity * (1 + pref_weighted_irr) AS pref_total_target,
    
    -- Common targets (ROC + IRR) 
    common_total_equity AS common_roc_target,
    common_total_equity * (1 + common_weighted_irr) AS common_total_target,
    
    -- Current portfolio IRR (simplified as cash/equity ratio)
    CASE 
      WHEN total_equity > 0 
      THEN (cumulative_cash_distributed / total_equity) - 1
      ELSE 0 
    END AS current_portfolio_irr
    
  FROM investor_balances
),

--------------------------------------------------------------------------------
-- STEP 3: Track cumulative payments to each tier
--------------------------------------------------------------------------------
cumulative_payments AS (
  SELECT
    *,
    
    -- Cumulative preferred payments so far
    COALESCE(
      SUM(
        LEAST(cash_available, pref_total_target)
      ) OVER (
        PARTITION BY portfolio_id 
        ORDER BY year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0
    ) AS cum_pref_paid_prior,
    
    -- Cumulative common payments so far  
    COALESCE(
      SUM(
        GREATEST(0, 
          LEAST(
            cash_available - LEAST(cash_available, pref_total_target),
            common_total_target
          )
        )
      ) OVER (
        PARTITION BY portfolio_id 
        ORDER BY year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0
    ) AS cum_common_paid_prior
    
  FROM tier_targets
),

--------------------------------------------------------------------------------
-- STEP 4: Calculate this year's waterfall distributions
--------------------------------------------------------------------------------
waterfall_calc AS (
  SELECT
    portfolio_id,
    year,
    total_cash_flow,
    cash_available,
    current_portfolio_irr,
    
    -- TIER 1: Preferred payments this year
    GREATEST(0,
      LEAST(
        total_cash_flow,
        GREATEST(0, pref_total_target - cum_pref_paid_prior)
      )
    ) AS pref_paid_this_year,
    
    -- Remaining cash after preferred
    GREATEST(0,
      total_cash_flow - 
      GREATEST(0,
        LEAST(
          total_cash_flow,
          GREATEST(0, pref_total_target - cum_pref_paid_prior)
        )
      )
    ) AS cash_after_pref,
    
    -- TIER 2: Common payments this year
    GREATEST(0,
      LEAST(
        GREATEST(0,
          total_cash_flow - 
          GREATEST(0,
            LEAST(
              total_cash_flow,
              GREATEST(0, pref_total_target - cum_pref_paid_prior)
            )
          )
        ),
        GREATEST(0, common_total_target - cum_common_paid_prior)
      )
    ) AS common_paid_this_year,
    
    -- Remaining cash after common (goes to promote tiers)
    GREATEST(0,
      total_cash_flow - 
      GREATEST(0,
        LEAST(
          total_cash_flow,
          GREATEST(0, pref_total_target - cum_pref_paid_prior)
        )
      ) -
      GREATEST(0,
        LEAST(
          GREATEST(0,
            total_cash_flow - 
            GREATEST(0,
              LEAST(
                total_cash_flow,
                GREATEST(0, pref_total_target - cum_pref_paid_prior)
              )
            )
          ),
          GREATEST(0, common_total_target - cum_common_paid_prior)
        )
      )
    ) AS promote_cash_available,
    
    -- Store key values for promote calculations
    pref_total_target,
    common_total_target,
    cum_pref_paid_prior,
    cum_common_paid_prior
    
  FROM cumulative_payments
),

--------------------------------------------------------------------------------
-- STEP 5: Allocate promote cash through hurdle tiers
--------------------------------------------------------------------------------
promote_allocation AS (
  SELECT
    w.*,
    
    -- Determine which hurdle tier we're in based on current IRR
    CASE 
      WHEN current_portfolio_irr <= 0.08 THEN 'hurdle1'
      WHEN current_portfolio_irr <= 0.12 THEN 'hurdle2' 
      WHEN current_portfolio_irr <= 0.18 THEN 'hurdle3'
      ELSE 'residual'
    END AS current_hurdle_tier,
    
    -- Get the appropriate splits for current tier
    CASE 
      WHEN current_portfolio_irr <= 0.08 THEN 0.7
      WHEN current_portfolio_irr <= 0.12 THEN 0.6
      WHEN current_portfolio_irr <= 0.18 THEN 0.5  
      ELSE 0.35
    END AS common_split_pct,
    
    CASE 
      WHEN current_portfolio_irr <= 0.08 THEN 0.3
      WHEN current_portfolio_irr <= 0.12 THEN 0.4
      WHEN current_portfolio_irr <= 0.18 THEN 0.5
      ELSE 0.65  
    END AS sponsor_split_pct
    
  FROM waterfall_calc AS w
)

--------------------------------------------------------------------------------
-- FINAL OUTPUT: Clean buckets for distribution
--------------------------------------------------------------------------------
SELECT
  portfolio_id,
  year,
  total_cash_flow,
  current_portfolio_irr,
  current_hurdle_tier,
  
  -- Preferred distributions
  pref_paid_this_year AS pref_total,
  
  -- Common distributions  
  common_paid_this_year AS common_total,
  
  -- Promote distributions by tier
  CASE WHEN current_hurdle_tier = 'hurdle1' 
       THEN ROUND(promote_cash_available * common_split_pct, 2) 
       ELSE 0 END AS h1_common,
       
  CASE WHEN current_hurdle_tier = 'hurdle1'
       THEN ROUND(promote_cash_available * sponsor_split_pct, 2)
       ELSE 0 END AS h1_sponsor,
       
  CASE WHEN current_hurdle_tier = 'hurdle2'
       THEN ROUND(promote_cash_available * common_split_pct, 2)
       ELSE 0 END AS h2_common,
       
  CASE WHEN current_hurdle_tier = 'hurdle2' 
       THEN ROUND(promote_cash_available * sponsor_split_pct, 2)
       ELSE 0 END AS h2_sponsor,
       
  CASE WHEN current_hurdle_tier = 'hurdle3'
       THEN ROUND(promote_cash_available * common_split_pct, 2) 
       ELSE 0 END AS h3_common,
       
  CASE WHEN current_hurdle_tier = 'hurdle3'
       THEN ROUND(promote_cash_available * sponsor_split_pct, 2)
       ELSE 0 END AS h3_sponsor,
       
  CASE WHEN current_hurdle_tier = 'residual'
       THEN ROUND(promote_cash_available * common_split_pct, 2)
       ELSE 0 END AS residual_common,
       
  CASE WHEN current_hurdle_tier = 'residual' 
       THEN ROUND(promote_cash_available * sponsor_split_pct, 2)
       ELSE 0 END AS residual_sponsor,
  
  -- Totals for verification
  pref_paid_this_year + common_paid_this_year + promote_cash_available AS total_distributed,
  promote_cash_available AS total_promote_cash

FROM promote_allocation
ORDER BY portfolio_id, year