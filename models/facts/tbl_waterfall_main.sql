{{
  config(
    materialized='view'
  )
}}

WITH 
--------------------------------------------------------------------------------
-- Get hurdle tiers configuration
--------------------------------------------------------------------------------
hurdle_tiers AS (
  SELECT
    hurdle_id,
    irr_range_high,
    investor_share,
    (1 - investor_share) AS sponsor_share
  FROM hkh_dev.tbl_hurdle_tiers
),

--------------------------------------------------------------------------------
-- Calculate target IRR threshold (weighted average across all investors)
-- UPDATED: Portfolio filtering
--------------------------------------------------------------------------------
target_irr_threshold AS (
  SELECT
    t.portfolio_id,
    SUM(t.target_irr * t.equity_contributed) / SUM(t.equity_contributed) AS weighted_avg_target_irr
  FROM hkh_dev.tbl_terms t
  INNER JOIN {{ source('inputs', 'portfolio_settings') }} ps 
    ON t.portfolio_id = ps.portfolio_id
  WHERE ps.company_id = 1  -- Company scoping for future multi-tenancy
    AND ps.is_default = TRUE  -- Only include default portfolio
  GROUP BY t.portfolio_id
),

--------------------------------------------------------------------------------
-- Get portfolio terms (aggregate once)
-- UPDATED: Portfolio filtering
--------------------------------------------------------------------------------
portfolio_terms AS (
  SELECT
    t.portfolio_id,
    SUM(CASE WHEN t.equity_class = 'Preferred' THEN t.equity_contributed ELSE 0 END) AS total_pref_equity,
    SUM(CASE WHEN t.equity_class = 'Common' THEN t.equity_contributed ELSE 0 END) AS total_common_equity,
    SUM(t.equity_contributed) AS total_equity,
    
    -- Weighted average base IRR for common investors
    CASE WHEN SUM(CASE WHEN t.equity_class = 'Common' THEN t.equity_contributed ELSE 0 END) > 0 THEN
      SUM(CASE WHEN t.equity_class = 'Common' THEN t.base_pref_irr * t.equity_contributed ELSE 0 END) / 
      SUM(CASE WHEN t.equity_class = 'Common' THEN t.equity_contributed ELSE 0 END)
    ELSE 0.07 END AS weighted_avg_base_irr,
    
    -- Join target IRR threshold
    tit.weighted_avg_target_irr
    
  FROM hkh_dev.tbl_terms t
  JOIN target_irr_threshold tit ON t.portfolio_id = tit.portfolio_id
  INNER JOIN {{ source('inputs', 'portfolio_settings') }} ps 
    ON t.portfolio_id = ps.portfolio_id
  WHERE ps.company_id = 1  -- Company scoping for future multi-tenancy
    AND ps.is_default = TRUE  -- Only include default portfolio
  GROUP BY t.portfolio_id, tit.weighted_avg_target_irr
),

--------------------------------------------------------------------------------
-- Base cash flows aggregated by portfolio and year
-- UPDATED: Use new base model and combine ATCF columns
-- NOTE: fact_property_cash_flow already has portfolio filtering
--------------------------------------------------------------------------------
base_data AS (
  SELECT
    cf.portfolio_id,
    cf.year,
    SUM(cf.atcf_operations) AS total_cash_flow,  -- UPDATED: Operations only, no refi proceeds
    pt.total_pref_equity,
    pt.total_common_equity,
    pt.total_equity,
    pt.weighted_avg_base_irr,
    pt.weighted_avg_target_irr
    
  FROM {{ ref('fact_property_cash_flow') }} cf  -- UPDATED: Use base model (already has portfolio filtering)
  JOIN portfolio_terms pt ON cf.portfolio_id = pt.portfolio_id
  GROUP BY cf.portfolio_id, cf.year, pt.total_pref_equity, pt.total_common_equity, pt.total_equity, pt.weighted_avg_base_irr, pt.weighted_avg_target_irr
),

--------------------------------------------------------------------------------
-- Add cumulative tracking
--------------------------------------------------------------------------------
cumulative_base AS (
  SELECT
    bd.*,
    
    -- Cumulative cash flow by portfolio
    SUM(bd.total_cash_flow) OVER (
      PARTITION BY bd.portfolio_id 
      ORDER BY bd.year 
      ROWS UNBOUNDED PRECEDING
    ) AS cumulative_cash_flow
    
  FROM base_data bd
),

--------------------------------------------------------------------------------
-- Calculate preferred ROC payments first
--------------------------------------------------------------------------------
pref_roc_step AS (
  SELECT
    cb.*,
    
    -- Preferred capital already returned in prior years
    GREATEST(0, LEAST(
      COALESCE(
        LAG(cb.cumulative_cash_flow, 1, 0) OVER (
          PARTITION BY cb.portfolio_id ORDER BY cb.year
        ), 0
      ),
      cb.total_pref_equity
    )) AS pref_roc_already_paid,
    
    -- Preferred ROC payment this period
    GREATEST(0, LEAST(
      cb.total_cash_flow,
      cb.total_pref_equity - GREATEST(0, LEAST(
        COALESCE(
          LAG(cb.cumulative_cash_flow, 1, 0) OVER (
            PARTITION BY cb.portfolio_id ORDER BY cb.year
          ), 0
        ),
        cb.total_pref_equity
      ))
    )) AS pref_roc_paid
    
  FROM cumulative_base cb
),

--------------------------------------------------------------------------------
-- Add LAG values for preferred calculations
--------------------------------------------------------------------------------
pref_lag_step AS (
  SELECT
    prs.*,
    LAG(prs.pref_roc_already_paid + prs.pref_roc_paid, 1, 0) OVER (
      PARTITION BY prs.portfolio_id ORDER BY prs.year
    ) AS prev_total_pref_roc_paid,
    LAG(prs.total_cash_flow, 1, 0) OVER (
      PARTITION BY prs.portfolio_id ORDER BY prs.year
    ) AS prev_total_cash_flow,
    LAG(prs.pref_roc_paid, 1, 0) OVER (
      PARTITION BY prs.portfolio_id ORDER BY prs.year
    ) AS prev_pref_roc_paid
  FROM pref_roc_step prs
),

--------------------------------------------------------------------------------
-- Calculate preferred IRR accrual and payments
--------------------------------------------------------------------------------
pref_irr_step AS (
  SELECT
    pls.*,
    
    -- Preferred capital outstanding
    GREATEST(0, pls.total_pref_equity - pls.pref_roc_already_paid - pls.pref_roc_paid) AS pref_capital_outstanding,
    
    -- Is preferred ROC complete?
    CASE WHEN (pls.pref_roc_already_paid + pls.pref_roc_paid) >= pls.total_pref_equity THEN 1 ELSE 0 END AS pref_roc_complete,
    
    -- Simplified IRR accrual - 8% on initial preferred equity for each year
    pls.total_pref_equity * 0.08 * pls.year AS total_pref_irr_target,
    
    -- IRR already paid in prior periods (simplified)
    COALESCE(
      SUM(
        CASE WHEN pls.prev_total_pref_roc_paid >= pls.total_pref_equity THEN
          GREATEST(0, pls.prev_total_cash_flow - pls.prev_pref_roc_paid)
        ELSE 0 END
      ) OVER (
        PARTITION BY pls.portfolio_id 
        ORDER BY pls.year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0
    ) AS pref_irr_already_paid,
    
    -- Cash available for preferred IRR this period
    GREATEST(0, pls.total_cash_flow - pls.pref_roc_paid) AS cash_after_pref_roc
    
  FROM pref_lag_step pls
),

--------------------------------------------------------------------------------
-- Calculate actual preferred IRR payment
--------------------------------------------------------------------------------
pref_final AS (
  SELECT
    pis.*,
    
    -- Preferred IRR payment this period
    CASE 
      WHEN pis.pref_roc_complete = 1 THEN
        GREATEST(0, LEAST(
          pis.cash_after_pref_roc,
          pis.total_pref_irr_target - pis.pref_irr_already_paid
        ))
      ELSE 0
    END AS pref_irr_paid,
    
    -- Cash remaining for common
    GREATEST(0, 
      pis.total_cash_flow - pis.pref_roc_paid - 
      CASE 
        WHEN pis.pref_roc_complete = 1 THEN
          GREATEST(0, LEAST(
            pis.cash_after_pref_roc,
            pis.total_pref_irr_target - pis.pref_irr_already_paid
          ))
        ELSE 0
      END
    ) AS cash_for_common
    
  FROM pref_irr_step pis
),

--------------------------------------------------------------------------------
-- Add LAG values for common calculations
--------------------------------------------------------------------------------
common_lag_step AS (
  SELECT
    pf.*,
    LAG(pf.pref_capital_outstanding, 1, pf.total_pref_equity) OVER (
      PARTITION BY pf.portfolio_id ORDER BY pf.year
    ) AS prev_pref_capital_outstanding_common_lag,
    LAG(pf.cash_for_common, 1, 0) OVER (
      PARTITION BY pf.portfolio_id ORDER BY pf.year
    ) AS prev_cash_for_common_common_lag
  FROM pref_final pf
),

--------------------------------------------------------------------------------
-- Calculate common ROC payments
--------------------------------------------------------------------------------
common_roc_step AS (
  SELECT
    cls.*,
    
    -- Common capital already returned
    COALESCE(
      SUM(
        CASE WHEN cls.prev_pref_capital_outstanding_common_lag = 0 THEN
          GREATEST(0, LEAST(cls.prev_cash_for_common_common_lag, cls.total_common_equity))
        ELSE 0 END
      ) OVER (
        PARTITION BY cls.portfolio_id 
        ORDER BY cls.year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0
    ) AS common_roc_already_paid,
    
    -- Common ROC payment this period
    CASE 
      WHEN cls.pref_capital_outstanding = 0 THEN
        GREATEST(0, LEAST(
          cls.cash_for_common,
          cls.total_common_equity - COALESCE(
            SUM(
              CASE WHEN cls.prev_pref_capital_outstanding_common_lag = 0 THEN
                GREATEST(0, LEAST(cls.prev_cash_for_common_common_lag, cls.total_common_equity))
              ELSE 0 END
            ) OVER (
              PARTITION BY cls.portfolio_id 
              ORDER BY cls.year 
              ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ), 0
          )
        ))
      ELSE 0
    END AS common_roc_paid
    
  FROM common_lag_step cls
),

--------------------------------------------------------------------------------
-- Add more LAG values for common IRR calculations
--------------------------------------------------------------------------------
common_irr_lag_step AS (
  SELECT
    crs.*,
    
    -- Is common ROC complete?
    CASE WHEN (crs.common_roc_already_paid + crs.common_roc_paid) >= crs.total_common_equity 
         AND crs.pref_capital_outstanding = 0 
    THEN 1 ELSE 0 END AS common_roc_complete,
    
    -- Cash remaining after common ROC
    GREATEST(0, crs.cash_for_common - crs.common_roc_paid) AS cash_after_common_roc,
    
    -- Common IRR accrual calculation - simple annual IRR
    CASE 
      WHEN (crs.common_roc_already_paid + crs.common_roc_paid) >= crs.total_common_equity 
           AND crs.pref_capital_outstanding = 0 THEN
        GREATEST(0, 
          crs.total_common_equity * crs.weighted_avg_base_irr * 
          (crs.year - 1)  -- Years elapsed since year 1
        )
      ELSE 0
    END AS total_common_irr_target,
    
    -- LAG values for IRR payment tracking  
    LAG(crs.common_roc_already_paid + crs.common_roc_paid, 1, 0) OVER (
      PARTITION BY crs.portfolio_id ORDER BY crs.year
    ) AS prev_total_common_roc_paid_irr_lag,
    LAG(crs.pref_capital_outstanding, 1, crs.total_pref_equity) OVER (
      PARTITION BY crs.portfolio_id ORDER BY crs.year
    ) AS prev_pref_capital_outstanding_irr_lag,
    LAG(crs.cash_for_common - crs.common_roc_paid, 1, 0) OVER (
      PARTITION BY crs.portfolio_id ORDER BY crs.year
    ) AS prev_cash_after_common_roc_irr_lag
    
  FROM common_roc_step crs
),

--------------------------------------------------------------------------------
-- Calculate common IRR and hurdle payments
--------------------------------------------------------------------------------
final_calculations AS (
  SELECT
    cils.*,
    
    -- Common IRR already paid in prior periods
    COALESCE(
      SUM(
        CASE WHEN cils.prev_total_common_roc_paid_irr_lag >= cils.total_common_equity 
        AND cils.prev_pref_capital_outstanding_irr_lag = 0 THEN
          GREATEST(0, cils.prev_cash_after_common_roc_irr_lag)
        ELSE 0 END
      ) OVER (
        PARTITION BY cils.portfolio_id 
        ORDER BY cils.year 
        ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
      ), 0
    ) AS common_irr_already_paid
    
  FROM common_irr_lag_step cils
),

--------------------------------------------------------------------------------
-- Calculate actual common IRR payment
--------------------------------------------------------------------------------
common_irr_step AS (
  SELECT
    fc.*,
    
    -- Common IRR payment this period (only after ROC complete)
    CASE 
      WHEN fc.common_roc_complete = 1 THEN
        GREATEST(0, LEAST(
          fc.cash_after_common_roc,
          fc.total_common_irr_target - fc.common_irr_already_paid
        ))
      ELSE 0
    END AS common_irr_paid
    
  FROM final_calculations fc
),

--------------------------------------------------------------------------------
-- Calculate hurdle payments with complete tier structure
--------------------------------------------------------------------------------
waterfall_final AS (
  SELECT
    cis.*,
    ht1.irr_range_high AS hurdle1_irr_threshold,
    ht1.investor_share AS hurdle1_investor_share,
    ht1.sponsor_share AS hurdle1_sponsor_share,
    ht2.irr_range_high AS hurdle2_irr_threshold,
    ht2.investor_share AS hurdle2_investor_share,
    ht2.sponsor_share AS hurdle2_sponsor_share,
    ht3.irr_range_high AS hurdle3_irr_threshold,
    ht3.investor_share AS hurdle3_investor_share,
    ht3.sponsor_share AS hurdle3_sponsor_share,
    htr.investor_share AS residual_investor_share,
    htr.sponsor_share AS residual_sponsor_share,
    
    -- Cash available for hurdles
    GREATEST(0, cis.cash_after_common_roc - cis.common_irr_paid) AS cash_for_hurdles,
    
    -- Calculate cumulative IRR achieved (simple, not compounding)
    CASE WHEN cis.total_equity > 0 THEN
      (cis.cumulative_cash_flow / cis.total_equity - 1) / GREATEST(1, cis.year - 1)
    ELSE 0 END AS cumulative_irr_achieved,
    
    -- Check if target IRR threshold has been met
    CASE WHEN cis.common_roc_complete = 1 AND 
         CASE WHEN cis.total_equity > 0 THEN
           (cis.cumulative_cash_flow / cis.total_equity - 1) / GREATEST(1, cis.year - 1)
         ELSE 0 END >= cis.weighted_avg_target_irr
    THEN 1 ELSE 0 END AS target_irr_met
    
  FROM common_irr_step cis
  CROSS JOIN (SELECT * FROM hurdle_tiers WHERE hurdle_id = 'hurdle1') ht1
  CROSS JOIN (SELECT * FROM hurdle_tiers WHERE hurdle_id = 'hurdle2') ht2  
  CROSS JOIN (SELECT * FROM hurdle_tiers WHERE hurdle_id = 'hurdle3') ht3
  CROSS JOIN (SELECT * FROM hurdle_tiers WHERE hurdle_id = 'residual') htr
),

--------------------------------------------------------------------------------
-- Calculate actual hurdle distributions
--------------------------------------------------------------------------------
hurdle_distributions AS (
  SELECT
    wf.*,
    
    -- If target IRR met, all remaining cash goes to sponsor
    CASE WHEN wf.target_irr_met = 1 THEN
      wf.cash_for_hurdles
    ELSE 0 END AS target_irr_excess_to_sponsor,
    
    -- Cash available if target IRR not met
    CASE WHEN wf.target_irr_met = 0 THEN wf.cash_for_hurdles ELSE 0 END AS cash_for_hurdle_tiers,
    
    -- Hurdle 1 calculations (only if target IRR not met)
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved <= wf.hurdle1_irr_threshold THEN
      GREATEST(0, wf.cash_for_hurdles * wf.hurdle1_investor_share)
    ELSE 0 END AS hurdle1_investor_calc,
    
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved <= wf.hurdle1_irr_threshold THEN
      GREATEST(0, wf.cash_for_hurdles * wf.hurdle1_sponsor_share)
    ELSE 0 END AS hurdle1_sponsor_calc,
    
    -- Cash remaining after hurdle 1
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved > wf.hurdle1_irr_threshold THEN
      wf.cash_for_hurdles
    ELSE 0 END AS cash_after_hurdle1,
    
    -- Hurdle 2 calculations
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved > wf.hurdle1_irr_threshold AND wf.cumulative_irr_achieved <= wf.hurdle2_irr_threshold THEN
      GREATEST(0, wf.cash_for_hurdles * wf.hurdle2_investor_share)
    ELSE 0 END AS hurdle2_investor_calc,
    
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved > wf.hurdle1_irr_threshold AND wf.cumulative_irr_achieved <= wf.hurdle2_irr_threshold THEN
      GREATEST(0, wf.cash_for_hurdles * wf.hurdle2_sponsor_share)
    ELSE 0 END AS hurdle2_sponsor_calc,
    
    -- Cash remaining after hurdle 2
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved > wf.hurdle2_irr_threshold THEN
      wf.cash_for_hurdles
    ELSE 0 END AS cash_after_hurdle2,
    
    -- Hurdle 3 calculations
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved > wf.hurdle2_irr_threshold AND wf.cumulative_irr_achieved <= wf.hurdle3_irr_threshold THEN
      GREATEST(0, wf.cash_for_hurdles * wf.hurdle3_investor_share)
    ELSE 0 END AS hurdle3_investor_calc,
    
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved > wf.hurdle2_irr_threshold AND wf.cumulative_irr_achieved <= wf.hurdle3_irr_threshold THEN
      GREATEST(0, wf.cash_for_hurdles * wf.hurdle3_sponsor_share)
    ELSE 0 END AS hurdle3_sponsor_calc,
    
    -- Residual calculations (anything above hurdle 3)
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved > wf.hurdle3_irr_threshold THEN
      GREATEST(0, wf.cash_for_hurdles * wf.residual_investor_share)
    ELSE 0 END AS residual_investor_calc,
    
    CASE WHEN wf.target_irr_met = 0 AND wf.common_roc_complete = 1 AND wf.cumulative_irr_achieved > wf.hurdle3_irr_threshold THEN
      GREATEST(0, wf.cash_for_hurdles * wf.residual_sponsor_share)
    ELSE 0 END AS residual_sponsor_calc
    
  FROM waterfall_final wf
)

--------------------------------------------------------------------------------
-- Final clean output without helper columns
--------------------------------------------------------------------------------
SELECT
  portfolio_id,
  year,
  ROUND(total_cash_flow, 2) AS total_cash_flow,
  
  -- Preferred distributions
  ROUND(pref_roc_paid, 2) AS pref_roc_paid,
  ROUND(pref_irr_paid, 2) AS pref_irr_paid,
  ROUND(pref_roc_paid + pref_irr_paid, 2) AS pref_total,
  
  -- Common distributions  
  ROUND(common_roc_paid, 2) AS common_roc_paid,
  ROUND(common_irr_paid, 2) AS common_irr_paid,
  
  -- Sponsor catchup (if any) - this could be calculated but I don't see it in the original model
  0 AS sponsor_catchup_paid,
  
  -- Hurdle distributions
  ROUND(hurdle1_investor_calc, 2) AS hurdle1_investor,
  ROUND(hurdle1_sponsor_calc, 2) AS hurdle1_sponsor,
  ROUND(hurdle2_investor_calc, 2) AS hurdle2_investor,
  ROUND(hurdle2_sponsor_calc, 2) AS hurdle2_sponsor,
  ROUND(hurdle3_investor_calc, 2) AS hurdle3_investor,
  ROUND(hurdle3_sponsor_calc, 2) AS hurdle3_sponsor,
  ROUND(residual_investor_calc, 2) AS residual_investor,
  ROUND(residual_sponsor_calc + target_irr_excess_to_sponsor, 2) AS residual_sponsor,
  
  -- Party totals (ALL distributions)
  ROUND(pref_roc_paid + pref_irr_paid + common_roc_paid + common_irr_paid + hurdle1_investor_calc + hurdle2_investor_calc + hurdle3_investor_calc + residual_investor_calc, 2) AS total_investor,
  ROUND(hurdle1_sponsor_calc + hurdle2_sponsor_calc + hurdle3_sponsor_calc + residual_sponsor_calc + target_irr_excess_to_sponsor, 2) AS total_sponsor,
  
  -- Validation
  ROUND(pref_roc_paid + pref_irr_paid + common_roc_paid + common_irr_paid + 
        hurdle1_investor_calc + hurdle1_sponsor_calc + hurdle2_investor_calc + hurdle2_sponsor_calc +
        hurdle3_investor_calc + hurdle3_sponsor_calc + residual_investor_calc + residual_sponsor_calc + 
        target_irr_excess_to_sponsor, 2) AS total_distributed,
  
  -- Additional fields that were in your CSV
  ROUND(total_cash_flow - (pref_roc_paid + pref_irr_paid + common_roc_paid + common_irr_paid + 
        hurdle1_investor_calc + hurdle1_sponsor_calc + hurdle2_investor_calc + hurdle2_sponsor_calc +
        hurdle3_investor_calc + hurdle3_sponsor_calc + residual_investor_calc + residual_sponsor_calc + 
        target_irr_excess_to_sponsor), 2) AS validation_difference,
  
  FALSE AS catchup_enabled,  -- Adjust this based on your actual catchup logic
  0.0 AS target_allocation   -- Adjust this based on your target allocation logic
  
FROM hurdle_distributions
ORDER BY portfolio_id, year