-- File: models/facts/fact_capex_with_incentives.sql
{{ config(materialized='table') }}

WITH base_capex AS (
  SELECT 
    c.property_id,
    c.year,
    c.spending_focus,
    c.rationale,
    r.capex_spent,
    r.available_for_capex,
    r.total_reserves_raised
  FROM {{ source('inputs', 'capex_factors') }} c
  LEFT JOIN {{ source('hkh_dev', 'capex_reserve_mgt') }} r 
    ON c.property_id = r.property_id 
    AND c.year = r.year
),

-- Expand spending focus to individual improvement categories
capex_with_categories AS (
  SELECT 
    c.*,
    UNNEST(m.improvement_categories) AS improvement_category
  FROM base_capex c
  LEFT JOIN {{ source('hkh_dev', 'capex_spending_focus_mapping') }} m ON c.spending_focus = m.spending_focus
),

-- Apply incentive programs
applicable_incentives AS (
  SELECT 
    cc.*,
    ip.program_id,
    ip.program_name,
    ip.incentive_structure,
    ip.incentive_rate,
    ip.incentive_cap,
    ip.tax_treatment,
    ip.cash_timing_days,
    ip.capture_rate_assumption,
    ip.tips_for_success,
    
    -- Calculate incentive amount
    CASE ip.incentive_structure
      WHEN 'percentage' THEN 
        LEAST(cc.capex_spent * ip.incentive_rate, COALESCE(ip.incentive_cap, 999999))
      WHEN 'fixed_amount' THEN 
        CASE WHEN cc.capex_spent >= COALESCE(ip.minimum_project_size, 0) 
             THEN ip.incentive_rate 
             ELSE 0 END
      WHEN 'per_sqft' THEN 
        ip.incentive_rate * 1000  -- Assuming 1000 sq ft average
    END AS potential_incentive_amount,
    
    -- Expected incentive after capture rate
    CASE ip.incentive_structure
      WHEN 'percentage' THEN 
        LEAST(cc.capex_spent * ip.incentive_rate, COALESCE(ip.incentive_cap, 999999)) * ip.capture_rate_assumption
      WHEN 'fixed_amount' THEN 
        CASE WHEN cc.capex_spent >= COALESCE(ip.minimum_project_size, 0) 
             THEN ip.incentive_rate * ip.capture_rate_assumption
             ELSE 0 END
      WHEN 'per_sqft' THEN 
        ip.incentive_rate * 1000 * ip.capture_rate_assumption
    END AS expected_incentive_amount
    
  FROM capex_with_categories cc
  LEFT JOIN {{ ref('incentive_programs') }} ip 
    ON ip.improvement_category = cc.improvement_category
    AND ip.funding_stability_score >= 6  -- Only include stable programs
),

-- Aggregate incentives per property/year
final_capex_with_incentives AS (
  SELECT 
    property_id,
    year,
    spending_focus,
    rationale,
    capex_spent,
    available_for_capex,
    
    -- Incentive summary
    COALESCE(SUM(expected_incentive_amount), 0) AS total_expected_incentives,
    COALESCE(SUM(CASE WHEN tax_treatment = 'tax_credit' THEN expected_incentive_amount ELSE 0 END), 0) AS tax_credit_value,
    COALESCE(SUM(CASE WHEN tax_treatment = 'reduces_basis' THEN expected_incentive_amount ELSE 0 END), 0) AS basis_reducing_rebates,
    
    -- Net financial impact
    capex_spent - COALESCE(SUM(expected_incentive_amount), 0) AS net_capex_cost,
    CASE WHEN capex_spent > 0 THEN (COALESCE(SUM(expected_incentive_amount), 0) / capex_spent) * 100 ELSE 0 END AS incentive_capture_percentage,
    
    -- Program details
    STRING_AGG(DISTINCT program_name, '; ') AS applicable_programs,
    STRING_AGG(DISTINCT tips_for_success, '; ') AS success_tips
    
  FROM applicable_incentives
  GROUP BY 1,2,3,4,5,6
)

SELECT * FROM final_capex_with_incentives
ORDER BY property_id, year