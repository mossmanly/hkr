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
-- Calculate waterfall distributions step by step
--------------------------------------------------------------------------------
waterfall_step1_pref_roc AS (
  SELECT
    acf.*,
    COALESCE(acf.prev_cumulative_cash_flow, 0) AS prev_cumulative_cash_flow_clean,
    
    -- Step 1: Preferred ROC
    GREATEST(0, LEAST(
      acf.total_cash_flow,
      GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0))
    )) AS pref_roc_paid,
    
    -- Remaining cash after preferred ROC
    GREATEST(0, acf.total_cash_flow - GREATEST(0, LEAST(
      acf.total_cash_flow,
      GREATEST(0, acf.total_pref_equity - COALESCE(acf.prev_cumulative_cash_flow, 0))
    ))) AS remaining_after_pref_roc
    
  FROM annual_cash_flows acf
),

--------------------------------------------------------------------------------
-- Step 2: Preferred IRR
--------------------------------------------------------------------------------
waterfall_step2_pref_irr AS (
  SELECT
    ws1.*,
    
    -- Preferred IRR bucket (cumulative target)
    ws1.total_pref_equity * ws1.weighted_avg_base_irr * ws1.year AS pref_irr_bucket,
    
    -- Step 2: Preferred IRR
    GREATEST(0, LEAST(
      ws1.remaining_after_pref_roc,
      GREATEST(0, ws1.total_pref_equity * ws1.weighted_avg_base_irr * ws1.year - 
        GREATEST(0, ws1.prev_cumulative_cash_flow_clean - ws1.total_pref_equity))
    )) AS pref_irr_paid,
    
    -- Remaining cash after preferred IRR
    GREATEST(0, ws1.remaining_after_pref_roc - GREATEST(0, LEAST(
      ws1.remaining_after_pref_roc,
      GREATEST(0, ws1.total_pref_equity * ws1.weighted_avg_base_irr * ws1.year - 
        GREATEST(0, ws1.prev_cumulative_cash_flow_clean - ws1.total_pref_equity))
    ))) AS remaining_after_pref_irr
    
  FROM waterfall_step1_pref_roc ws1
),

--------------------------------------------------------------------------------
-- Step 3: Common ROC
--------------------------------------------------------------------------------
waterfall_step3_common_roc AS (
  SELECT
    ws2.*,
    
    -- Step 3: Common ROC
    GREATEST(0, LEAST(
      ws2.remaining_after_pref_irr,
      GREATEST(0, ws2.total_common_equity - 
        GREATEST(0, ws2.prev_cumulative_cash_flow_clean - ws2.total_pref_equity - ws2.pref_irr_bucket))
    )) AS common_roc_paid,
    
    -- Remaining cash after common ROC
    GREATEST(0, ws2.remaining_after_pref_irr - GREATEST(0, LEAST(
      ws2.remaining_after_pref_irr,
      GREATEST(0, ws2.total_common_equity - 
        GREATEST(0, ws2.prev_cumulative_cash_flow_clean - ws2.total_pref_equity - ws2.pref_irr_bucket))
    ))) AS remaining_after_common_roc
    
  FROM waterfall_step2_pref_irr ws2
),

--------------------------------------------------------------------------------
-- Step 4: Common IRR
--------------------------------------------------------------------------------
waterfall_step4_common_irr AS (
  SELECT
    ws3.*,
    
    -- Common IRR bucket (cumulative target)
    ws3.total_common_equity * ws3.weighted_avg_base_irr * ws3.year AS common_irr_bucket,
    
    -- Step 4: Common IRR
    GREATEST(0, LEAST(
      ws3.remaining_after_common_roc,
      GREATEST(0, ws3.total_common_equity * ws3.weighted_avg_base_irr * ws3.year - 
        GREATEST(0, ws3.prev_cumulative_cash_flow_clean - ws3.total_pref_equity - ws3.pref_irr_bucket - ws3.total_common_equity))
    )) AS common_irr_paid,
    
    -- Remaining cash for hurdles (NO SPONSOR CATCHUP YET)
    GREATEST(0, ws3.remaining_after_common_roc - GREATEST(0, LEAST(
      ws3.remaining_after_common_roc,
      GREATEST(0, ws3.total_common_equity * ws3.weighted_avg_base_irr * ws3.year - 
        GREATEST(0, ws3.prev_cumulative_cash_flow_clean - ws3.total_pref_equity - ws3.pref_irr_bucket - ws3.total_common_equity))
    ))) AS cash_for_hurdles
    
  FROM waterfall_step3_common_roc ws3
),

--------------------------------------------------------------------------------
-- Step 5: Sponsor Catchup (NEW CLEAN TIER)
--------------------------------------------------------------------------------
waterfall_step5_sponsor_catchup AS (
  SELECT
    ws4.*,
    
    -- Calculate total investor distributions so far (preferred + common total)
    GREATEST(0, 
      LEAST(ws4.prev_cumulative_cash_flow_clean + ws4.pref_roc_paid + ws4.pref_irr_paid + ws4.common_roc_paid + ws4.common_irr_paid,
            ws4.total_pref_equity + ws4.pref_irr_bucket + ws4.total_common_equity + ws4.common_irr_bucket)
    ) AS total_investor_distributions,
    
    -- Sponsor catchup: simplified trigger - start after common ROC is complete AND before target IRR met
    CASE WHEN ws4.catchup_enabled = TRUE AND 
              ws4.catchup_timing = 'after_common_irr' AND
              -- Simple trigger: common ROC bucket is filled
              (ws4.prev_cumulative_cash_flow_clean + ws4.pref_roc_paid + ws4.pref_irr_paid + ws4.common_roc_paid) >= 
              (ws4.total_pref_equity + ws4.pref_irr_bucket + ws4.total_common_equity) AND
              -- And there's cash available
              ws4.cash_for_hurdles > 0 AND
              -- STOP catchup once target IRR is met
              CASE WHEN ws4.total_equity > 0 AND ws4.year > 1 THEN
                (ws4.cumulative_cash_flow / ws4.total_equity - 1) / (ws4.year - 1) < ws4.weighted_avg_target_irr
              ELSE TRUE END THEN
      
      -- Give sponsor their target percentage of available cash (simplified)
      ws4.cash_for_hurdles * COALESCE(ws4.target_sponsor_allocation, 0.20)
      
    ELSE 0 END AS sponsor_catchup_paid,
    
    -- Remaining cash for hurdles after sponsor catchup (simplified)
    GREATEST(0, ws4.cash_for_hurdles - 
      CASE WHEN ws4.catchup_enabled = TRUE AND 
                ws4.catchup_timing = 'after_common_irr' AND
                (ws4.prev_cumulative_cash_flow_clean + ws4.pref_roc_paid + ws4.pref_irr_paid + ws4.common_roc_paid) >= 
                (ws4.total_pref_equity + ws4.pref_irr_bucket + ws4.total_common_equity) AND
                ws4.cash_for_hurdles > 0 AND
                CASE WHEN ws4.total_equity > 0 AND ws4.year > 1 THEN
                  (ws4.cumulative_cash_flow / ws4.total_equity - 1) / (ws4.year - 1) < ws4.weighted_avg_target_irr
                ELSE TRUE END THEN
        ws4.cash_for_hurdles * COALESCE(ws4.target_sponsor_allocation, 0.20)
      ELSE 0 END
    ) AS remaining_for_hurdles
    
  FROM waterfall_step4_common_irr ws4
),

--------------------------------------------------------------------------------
-- Step 6: FIXED HURDLES - Separate calculations for each tier
--------------------------------------------------------------------------------
final_waterfall AS (
  SELECT
    ws5.*,
    
    -- Calculate current IRR for hurdle determination
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 THEN
      (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1)
    ELSE 0 END AS current_irr,
    
    -- Check if target IRR threshold has been met
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) >= ws5.weighted_avg_target_irr
    THEN 1 ELSE 0 END AS target_irr_met,
    
    -- FIXED: Separate each hurdle tier calculation
    -- Hurdle 1 (under 20% IRR)
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < 0.20 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < ws5.weighted_avg_target_irr THEN
      ws5.remaining_for_hurdles * COALESCE(ws5.hurdle1_investor_share, 0.70)
    ELSE 0 END AS hurdle1_investor,
    
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < 0.20 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < ws5.weighted_avg_target_irr THEN
      ws5.remaining_for_hurdles * COALESCE(ws5.hurdle1_sponsor_share, 0.30)
    ELSE 0 END AS hurdle1_sponsor,
    
    -- Hurdle 2 (20%+ to 30% IRR)
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) >= 0.20 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < 0.30 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < ws5.weighted_avg_target_irr THEN
      ws5.remaining_for_hurdles * COALESCE(ws5.hurdle2_investor_share, 0.60)
    ELSE 0 END AS hurdle2_investor,
    
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) >= 0.20 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < 0.30 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < ws5.weighted_avg_target_irr THEN
      ws5.remaining_for_hurdles * COALESCE(ws5.hurdle2_sponsor_share, 0.40)
    ELSE 0 END AS hurdle2_sponsor,
    
    -- Hurdle 3 (30%+ to 40% IRR)
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) >= 0.30 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < 0.40 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < ws5.weighted_avg_target_irr THEN
      ws5.remaining_for_hurdles * COALESCE(ws5.hurdle3_investor_share, 0.50)
    ELSE 0 END AS hurdle3_investor,
    
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) >= 0.30 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < 0.40 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < ws5.weighted_avg_target_irr THEN
      ws5.remaining_for_hurdles * COALESCE(ws5.hurdle3_sponsor_share, 0.50)
    ELSE 0 END AS hurdle3_sponsor,
    
    -- Residual (40%+ IRR, but before target IRR cutoff)
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) >= 0.40 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < ws5.weighted_avg_target_irr THEN
      ws5.remaining_for_hurdles * COALESCE(ws5.residual_investor_share, 0.35)
    ELSE 0 END AS residual_investor,
    
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) >= 0.40 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) < ws5.weighted_avg_target_irr THEN
      ws5.remaining_for_hurdles * COALESCE(ws5.residual_sponsor_share, 0.65)
    ELSE 0 END AS residual_sponsor,
    
    -- Target IRR met: ALL remaining cash to sponsor
    CASE WHEN ws5.total_equity > 0 AND ws5.year > 1 AND
         (ws5.cumulative_cash_flow / ws5.total_equity - 1) / (ws5.year - 1) >= ws5.weighted_avg_target_irr THEN
      ws5.remaining_for_hurdles
    ELSE 0 END AS target_irr_sponsor
    
  FROM waterfall_step5_sponsor_catchup ws5
)

--------------------------------------------------------------------------------
-- FIXED: Final clean output with properly separated hurdle distributions
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
  
  -- FIXED: Properly separated hurdle distributions
  ROUND(hurdle1_investor, 0) AS hurdle1_investor,
  ROUND(hurdle1_sponsor, 0) AS hurdle1_sponsor,
  ROUND(hurdle2_investor, 0) AS hurdle2_investor,
  ROUND(hurdle2_sponsor, 0) AS hurdle2_sponsor,
  ROUND(hurdle3_investor, 0) AS hurdle3_investor,
  ROUND(hurdle3_sponsor, 0) AS hurdle3_sponsor,
  ROUND(residual_investor, 0) AS residual_investor,
  ROUND(residual_sponsor, 0) AS residual_sponsor,
  ROUND(target_irr_sponsor, 0) AS target_irr_sponsor,
  
  -- Party totals (FIXED: include all hurdle tiers)
  ROUND(pref_roc_paid + pref_irr_paid + common_roc_paid + common_irr_paid + 
        hurdle1_investor + hurdle2_investor + hurdle3_investor + residual_investor, 0) AS total_investor,
  ROUND(sponsor_catchup_paid + hurdle1_sponsor + hurdle2_sponsor + hurdle3_sponsor + 
        residual_sponsor + target_irr_sponsor, 0) AS total_sponsor,
  
  -- Validation (FIXED: include all distributions)
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
  target_irr_met AS target_irr_achieved,
  
  -- ADDED: Debug which tier is being used
  CASE 
    WHEN current_irr >= weighted_avg_target_irr THEN 'TARGET_IRR_MET'
    WHEN current_irr >= 0.40 THEN 'RESIDUAL'
    WHEN current_irr >= 0.30 THEN 'HURDLE3'
    WHEN current_irr >= 0.20 THEN 'HURDLE2'
    ELSE 'HURDLE1'
  END AS debug_active_tier

FROM final_waterfall
ORDER BY portfolio_id, year