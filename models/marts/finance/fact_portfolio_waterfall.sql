{{ config(materialized='view') }}

WITH 
--------------------------------------------------------------------------------
-- Get target IRR threshold
--------------------------------------------------------------------------------
target_irr_threshold AS (
  SELECT
    portfolio_id,
    -- Use weighted average target_irr from stg_terms as the cutoff point
    CASE WHEN SUM(equity_contributed) > 0 THEN
      SUM(target_irr * equity_contributed) / SUM(equity_contributed)
    ELSE 0.12 END AS weighted_avg_target_irr
  FROM {{ source('hkh_dev', 'stg_terms') }}
  WHERE portfolio_id IN (
    SELECT DISTINCT t.portfolio_id 
    FROM {{ source('hkh_dev', 'stg_terms') }} t
    INNER JOIN {{ source('hkh_dev', 'stg_portfolio_settings') }} ps
      ON t.portfolio_id = ps.portfolio_id
    WHERE ps.company_id = 1 AND ps.is_default = TRUE
  )
  GROUP BY portfolio_id
),

--------------------------------------------------------------------------------
-- Get portfolio terms and hurdle settings + sponsor catchup settings
--------------------------------------------------------------------------------
portfolio_terms AS (
  SELECT
    t.portfolio_id,
    SUM(CASE WHEN t.equity_class = 'Preferred' THEN t.equity_contributed ELSE 0 END) AS total_pref_equity,
    SUM(CASE WHEN t.equity_class = 'Common' THEN t.equity_contributed ELSE 0 END) AS total_common_equity,
    SUM(t.equity_contributed) AS total_equity,
    
    -- Weighted average base IRR for ALL equity (preferred AND common)
    CASE WHEN SUM(t.equity_contributed) > 0 THEN
      SUM(t.base_pref_irr * t.equity_contributed) / SUM(t.equity_contributed)
    ELSE 0.07 END AS weighted_avg_base_irr,
    
    tit.weighted_avg_target_irr,
    
    -- Sponsor catchup settings
    cws.catchup_enabled,
    cws.target_sponsor_allocation,
    cws.catchup_timing,
    cws.annual_catchup_cap,
    
    -- Get hurdle settings from stg_hurdle_tiers
    h1.irr_range_high AS hurdle1_threshold,
    h1.investor_share AS hurdle1_investor_share,
    (1 - h1.investor_share) AS hurdle1_sponsor_share,
    h2.irr_range_high AS hurdle2_threshold,
    h2.investor_share AS hurdle2_investor_share,
    (1 - h2.investor_share) AS hurdle2_sponsor_share,
    h3.irr_range_high AS hurdle3_threshold,
    h3.investor_share AS hurdle3_investor_share,
    (1 - h3.investor_share) AS hurdle3_sponsor_share,
    hr.investor_share AS residual_investor_share,
    (1 - hr.investor_share) AS residual_sponsor_share
    
  FROM {{ source('hkh_dev', 'stg_terms') }} t
  JOIN target_irr_threshold tit ON t.portfolio_id = tit.portfolio_id
  INNER JOIN {{ source('hkh_dev', 'stg_portfolio_settings') }} ps
    ON t.portfolio_id = ps.portfolio_id
  LEFT JOIN inputs.company_waterfall_settings cws ON ps.company_id::text = cws.company_id
  LEFT JOIN {{ source('hkh_dev', 'stg_hurdle_tiers') }} h1
    ON h1.hurdle_id = 'hurdle1'
  LEFT JOIN {{ source('hkh_dev', 'stg_hurdle_tiers') }} h2
    ON h2.hurdle_id = 'hurdle2'
  LEFT JOIN {{ source('hkh_dev', 'stg_hurdle_tiers') }} h3
    ON h3.hurdle_id = 'hurdle3'
  LEFT JOIN {{ source('hkh_dev', 'stg_hurdle_tiers') }} hr
    ON hr.hurdle_id = 'residual'
  WHERE ps.company_id = 1
    AND ps.is_default = TRUE
  GROUP BY t.portfolio_id, tit.weighted_avg_target_irr, cws.catchup_enabled, cws.target_sponsor_allocation, cws.catchup_timing, cws.annual_catchup_cap, h1.irr_range_high, h1.investor_share, h2.irr_range_high, h2.investor_share, h3.irr_range_high, h3.investor_share, hr.investor_share
),

--------------------------------------------------------------------------------
-- Aggregate annual cash flows
--------------------------------------------------------------------------------
annual_cash_flows AS (
  SELECT
    cf.portfolio_id,
    cf.year,
    pt.total_pref_equity,
    pt.total_common_equity,
    pt.total_equity,
    pt.weighted_avg_base_irr,
    pt.weighted_avg_target_irr,
    pt.catchup_enabled,
    pt.target_sponsor_allocation,
    pt.catchup_timing,
    pt.annual_catchup_cap,
    pt.hurdle1_threshold,
    pt.hurdle1_investor_share,
    pt.hurdle1_sponsor_share,
    pt.hurdle2_threshold,
    pt.hurdle2_investor_share,
    pt.hurdle2_sponsor_share,
    pt.hurdle3_threshold,
    pt.hurdle3_investor_share,
    pt.hurdle3_sponsor_share,
    pt.residual_investor_share,
    pt.residual_sponsor_share,
    
    SUM(cf.atcf_operations) AS total_cash_flow,
    SUM(SUM(cf.atcf_operations)) OVER (
      PARTITION BY cf.portfolio_id 
      ORDER BY cf.year 
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_cash_flow,
    SUM(SUM(cf.atcf_operations)) OVER (
      PARTITION BY cf.portfolio_id 
      ORDER BY cf.year 
      ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
    ) AS prev_cumulative_cash_flow
    
  FROM {{ ref('int_property_cash_flows') }} cf
  JOIN portfolio_terms pt ON cf.portfolio_id = pt.portfolio_id
  GROUP BY cf.portfolio_id, cf.year, pt.total_pref_equity, pt.total_common_equity, pt.total_equity, pt.weighted_avg_base_irr, pt.weighted_avg_target_irr, pt.catchup_enabled, pt.target_sponsor_allocation, pt.catchup_timing, pt.annual_catchup_cap, pt.hurdle1_threshold, pt.hurdle1_investor_share, pt.hurdle1_sponsor_share, pt.hurdle2_threshold, pt.hurdle2_investor_share, pt.hurdle2_sponsor_share, pt.hurdle3_threshold, pt.hurdle3_investor_share, pt.hurdle3_sponsor_share, pt.residual_investor_share, pt.residual_sponsor_share
),

--------------------------------------------------------------------------------
-- FIXED: Sequential waterfall with proper cash flow reduction at each step
--------------------------------------------------------------------------------
final_waterfall AS (
  SELECT
    acf.*,
    COALESCE(acf.prev_cumulative_cash_flow, 0) AS prev_cumulative_cash_flow_clean,
    
    -- Calculate current IRR for hurdle determination
    CASE WHEN acf.total_equity > 0 AND acf.year > 1 THEN
      (acf.cumulative_cash_flow / acf.total_equity - 1) / (acf.year - 1)
    ELSE 0 END AS current_irr,
    
    -- Step 1: Preferred ROC
    GREATEST(0, LEAST(
      acf.total_cash_flow,
      GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0))
    )) AS pref_roc_paid,
    
    -- Step 2: Preferred IRR (from remaining cash after pref ROC)
    GREATEST(0, LEAST(
      acf.total_cash_flow - GREATEST(0, LEAST(
        acf.total_cash_flow,
        GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0))
      )),
      GREATEST(0, acf.total_pref_equity * acf.weighted_avg_base_irr * acf.year - 
        GREATEST(0, COALESCE(acf.prev_cumulative_cash_flow, 0) - acf.total_pref_equity))
    )) AS pref_irr_paid,
    
    -- Step 3: Common ROC (from remaining cash after pref distributions)
    GREATEST(0, LEAST(
      acf.total_cash_flow - 
      GREATEST(0, LEAST(acf.total_cash_flow, GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0)))) -
      GREATEST(0, LEAST(
        acf.total_cash_flow - GREATEST(0, LEAST(acf.total_cash_flow, GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0)))),
        GREATEST(0, acf.total_pref_equity * acf.weighted_avg_base_irr * acf.year - GREATEST(0, COALESCE(acf.prev_cumulative_cash_flow, 0) - acf.total_pref_equity))
      )),
      GREATEST(0, acf.total_common_equity - 
        GREATEST(0, COALESCE(acf.prev_cumulative_cash_flow, 0) - acf.total_pref_equity - acf.total_pref_equity * acf.weighted_avg_base_irr * acf.year))
    )) AS common_roc_paid,
    
    -- Step 4: Common IRR (from remaining cash after common ROC)
    GREATEST(0, LEAST(
      acf.total_cash_flow - 
      -- Subtract all previous distributions
      GREATEST(0, LEAST(acf.total_cash_flow, GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0)))) -
      GREATEST(0, LEAST(
        acf.total_cash_flow - GREATEST(0, LEAST(acf.total_cash_flow, GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0)))),
        GREATEST(0, acf.total_pref_equity * acf.weighted_avg_base_irr * acf.year - GREATEST(0, COALESCE(acf.prev_cumulative_cash_flow, 0) - acf.total_pref_equity))
      )) -
      GREATEST(0, LEAST(
        acf.total_cash_flow - 
        GREATEST(0, LEAST(acf.total_cash_flow, GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0)))) -
        GREATEST(0, LEAST(
          acf.total_cash_flow - GREATEST(0, LEAST(acf.total_cash_flow, GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0)))),
          GREATEST(0, acf.total_pref_equity * acf.weighted_avg_base_irr * acf.year - GREATEST(0, COALESCE(acf.prev_cumulative_cash_flow, 0) - acf.total_pref_equity))
        )),
        GREATEST(0, acf.total_common_equity - 
          GREATEST(0, COALESCE(acf.prev_cumulative_cash_flow, 0) - acf.total_pref_equity - acf.total_pref_equity * acf.weighted_avg_base_irr * acf.year))
      )),
      GREATEST(0, acf.total_common_equity * acf.weighted_avg_base_irr * acf.year - 
        GREATEST(0, COALESCE(acf.prev_cumulative_cash_flow, 0) - acf.total_pref_equity - acf.total_pref_equity * acf.weighted_avg_base_irr * acf.year - acf.total_common_equity))
    )) AS common_irr_paid
    
  FROM annual_cash_flows acf
),

-- Calculate remaining cash after basic distributions
remaining_cash_calc AS (
  SELECT
    fw.*,
    -- Calculate remaining cash for hurdles/catchup
    GREATEST(0, fw.total_cash_flow - fw.pref_roc_paid - fw.pref_irr_paid - fw.common_roc_paid - fw.common_irr_paid) AS remaining_for_hurdles_and_catchup
  FROM final_waterfall fw
),

-- Calculate sponsor catchup
sponsor_catchup_calc AS (
  SELECT
    rcc.*,
    -- Sponsor catchup calculation
    CASE WHEN rcc.catchup_enabled = TRUE AND 
              rcc.catchup_timing = 'after_common_irr' AND
              (rcc.prev_cumulative_cash_flow_clean + rcc.pref_roc_paid + rcc.pref_irr_paid + rcc.common_roc_paid) >= 
              (rcc.total_pref_equity + rcc.total_pref_equity * rcc.weighted_avg_base_irr * rcc.year + rcc.total_common_equity) AND
              rcc.remaining_for_hurdles_and_catchup > 0 AND
              CASE WHEN rcc.total_equity > 0 AND rcc.year > 1 THEN
                rcc.current_irr < rcc.weighted_avg_target_irr
              ELSE TRUE END THEN
      rcc.remaining_for_hurdles_and_catchup * COALESCE(rcc.target_sponsor_allocation, 0.20)
    ELSE 0 END AS sponsor_catchup_paid,
    
    -- Remaining for hurdles after sponsor catchup
    GREATEST(0, rcc.remaining_for_hurdles_and_catchup - 
      CASE WHEN rcc.catchup_enabled = TRUE AND 
                rcc.catchup_timing = 'after_common_irr' AND
                (rcc.prev_cumulative_cash_flow_clean + rcc.pref_roc_paid + rcc.pref_irr_paid + rcc.common_roc_paid) >= 
                (rcc.total_pref_equity + rcc.total_pref_equity * rcc.weighted_avg_base_irr * rcc.year + rcc.total_common_equity) AND
                rcc.remaining_for_hurdles_and_catchup > 0 AND
                CASE WHEN rcc.total_equity > 0 AND rcc.year > 1 THEN
                  rcc.current_irr < rcc.weighted_avg_target_irr
                ELSE TRUE END THEN
        rcc.remaining_for_hurdles_and_catchup * COALESCE(rcc.target_sponsor_allocation, 0.20)
      ELSE 0 END
    ) AS remaining_for_hurdles
    
  FROM remaining_cash_calc rcc
),

-- Calculate hurdle distributions
hurdle_distributions AS (
  SELECT
    scc.*,
    
    -- Hurdle 1 (under hurdle1_threshold)
    CASE WHEN scc.total_equity > 0 AND scc.year > 1 AND
         scc.current_irr < scc.hurdle1_threshold AND
         scc.current_irr < scc.weighted_avg_target_irr THEN
      scc.remaining_for_hurdles * COALESCE(scc.hurdle1_investor_share, 0.70)
    ELSE 0 END AS hurdle1_investor,
    
    CASE WHEN scc.total_equity > 0 AND scc.year > 1 AND
         scc.current_irr < scc.hurdle1_threshold AND
         scc.current_irr < scc.weighted_avg_target_irr THEN
      scc.remaining_for_hurdles * COALESCE(scc.hurdle1_sponsor_share, 0.30)
    ELSE 0 END AS hurdle1_sponsor,
    
    -- Hurdle 2 (hurdle1_threshold to hurdle2_threshold)
    CASE WHEN scc.total_equity > 0 AND scc.year > 1 AND
         scc.current_irr >= scc.hurdle1_threshold AND
         scc.current_irr < scc.hurdle2_threshold AND
         scc.current_irr < scc.weighted_avg_target_irr THEN
      scc.remaining_for_hurdles * COALESCE(scc.hurdle2_investor_share, 0.60)
    ELSE 0 END AS hurdle2_investor,
    
    CASE WHEN scc.total_equity > 0 AND scc.year > 1 AND
         scc.current_irr >= scc.hurdle1_threshold AND
         scc.current_irr < scc.hurdle2_threshold AND
         scc.current_irr < scc.weighted_avg_target_irr THEN
      scc.remaining_for_hurdles * COALESCE(scc.hurdle2_sponsor_share, 0.40)
    ELSE 0 END AS hurdle2_sponsor,
    
    -- Hurdle 3 (hurdle2_threshold to hurdle3_threshold)
    CASE WHEN scc.total_equity > 0 AND scc.year > 1 AND
         scc.current_irr >= scc.hurdle2_threshold AND
         scc.current_irr < scc.hurdle3_threshold AND
         scc.current_irr < scc.weighted_avg_target_irr THEN
      scc.remaining_for_hurdles * COALESCE(scc.hurdle3_investor_share, 0.50)
    ELSE 0 END AS hurdle3_investor,
    
    CASE WHEN scc.total_equity > 0 AND scc.year > 1 AND
         scc.current_irr >= scc.hurdle2_threshold AND
         scc.current_irr < scc.hurdle3_threshold AND
         scc.current_irr < scc.weighted_avg_target_irr THEN
      scc.remaining_for_hurdles * COALESCE(scc.hurdle3_sponsor_share, 0.50)
    ELSE 0 END AS hurdle3_sponsor,
    
    -- Residual (hurdle3_threshold to weighted_avg_target_irr)
    CASE WHEN scc.total_equity > 0 AND scc.year > 1 AND
         scc.current_irr >= scc.hurdle3_threshold AND
         scc.current_irr < scc.weighted_avg_target_irr THEN
      scc.remaining_for_hurdles * COALESCE(scc.residual_investor_share, 0.35)
    ELSE 0 END AS residual_investor,
    
    CASE WHEN scc.total_equity > 0 AND scc.year > 1 AND
         scc.current_irr >= scc.hurdle3_threshold AND
         scc.current_irr < scc.weighted_avg_target_irr THEN
      scc.remaining_for_hurdles * COALESCE(scc.residual_sponsor_share, 0.65)
    ELSE 0 END AS residual_sponsor,
    
    -- Target IRR met: ALL remaining cash to sponsor
    CASE WHEN scc.total_equity > 0 AND scc.year > 1 AND
         scc.current_irr >= scc.weighted_avg_target_irr THEN
      scc.remaining_for_hurdles
    ELSE 0 END AS target_irr_sponsor
    
  FROM sponsor_catchup_calc scc
)

--------------------------------------------------------------------------------
-- Final clean output
--------------------------------------------------------------------------------
SELECT
  portfolio_id,
  year,
  ROUND(total_cash_flow, 0) AS total_cash_flow,
  
  -- Basic distributions
  ROUND(pref_roc_paid, 0) AS pref_roc_paid,
  ROUND(pref_irr_paid, 0) AS pref_irr_paid,
  ROUND(pref_roc_paid + pref_irr_paid, 0) AS pref_total,
  ROUND(common_roc_paid, 0) AS common_roc_paid,
  ROUND(common_irr_paid, 0) AS common_irr_paid,
  
  -- Sponsor catchup
  ROUND(sponsor_catchup_paid, 0) AS sponsor_catchup_paid,
  
  -- Hurdle distributions
  ROUND(hurdle1_investor, 0) AS hurdle1_investor,
  ROUND(hurdle1_sponsor, 0) AS hurdle1_sponsor,
  ROUND(hurdle2_investor, 0) AS hurdle2_investor,
  ROUND(hurdle2_sponsor, 0) AS hurdle2_sponsor,
  ROUND(hurdle3_investor, 0) AS hurdle3_investor,
  ROUND(hurdle3_sponsor, 0) AS hurdle3_sponsor,
  ROUND(residual_investor, 0) AS residual_investor,
  ROUND(residual_sponsor, 0) AS residual_sponsor,
  ROUND(target_irr_sponsor, 0) AS target_irr_sponsor,
  
  -- Party totals
  ROUND(pref_roc_paid + pref_irr_paid + common_roc_paid + common_irr_paid + 
        hurdle1_investor + hurdle2_investor + hurdle3_investor + residual_investor, 0) AS total_investor,
  ROUND(sponsor_catchup_paid + hurdle1_sponsor + hurdle2_sponsor + hurdle3_sponsor + 
        residual_sponsor + target_irr_sponsor, 0) AS total_sponsor,
  
  -- Validation
  ROUND(pref_roc_paid + pref_irr_paid + common_roc_paid + common_irr_paid + sponsor_catchup_paid + 
        hurdle1_investor + hurdle1_sponsor + hurdle2_investor + hurdle2_sponsor + 
        hurdle3_investor + hurdle3_sponsor + residual_investor + residual_sponsor + 
        target_irr_sponsor, 0) AS total_distributed,
  ROUND(total_cash_flow - (pref_roc_paid + pref_irr_paid + common_roc_paid + common_irr_paid + sponsor_catchup_paid + 
        hurdle1_investor + hurdle1_sponsor + hurdle2_investor + hurdle2_sponsor + 
        hurdle3_investor + hurdle3_sponsor + residual_investor + residual_sponsor + 
        target_irr_sponsor), 0) AS validation_difference,
  
  -- Debug info
  ROUND(current_irr, 4) AS current_irr,
  ROUND(weighted_avg_target_irr, 4) AS target_irr_threshold,
  CASE WHEN current_irr >= weighted_avg_target_irr THEN 1 ELSE 0 END AS target_irr_achieved,
  
  -- Debug which tier is being used with actual thresholds
  CASE 
    WHEN current_irr >= weighted_avg_target_irr THEN 'TARGET_IRR_MET'
    WHEN current_irr >= hurdle3_threshold THEN 'RESIDUAL'
    WHEN current_irr >= hurdle2_threshold THEN 'HURDLE3'
    WHEN current_irr >= hurdle1_threshold THEN 'HURDLE2'
    ELSE 'HURDLE1'
  END AS debug_active_tier

FROM hurdle_distributions
ORDER BY portfolio_id, year