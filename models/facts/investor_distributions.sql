-- models/facts/investor_distributions.sql
-- dbt model for investor distributions by investor and year
-- Suppresses periods with zero distributions

{{ config(materialized='view') }}

SELECT 
    -- Primary Keys First
    t.portfolio_id,
    t.investor_serial,
    w.year,
    
    -- Investor Information
    t.first_name,
    t.last_name,
    t.full_name,
    t.company_name,
    
    -- Individual Investor Distribution Calculations
    -- ROC (Return of Capital)
    ROUND((w.pref_roc_paid + w.common_roc_paid) * t.percentage_of_investments, 2) AS investor_roc,
    
    -- Base/Preferred IRR
    ROUND(w.pref_irr_paid * t.percentage_of_investments, 2) AS investor_base_pref_irr,
    
    -- Hurdle Distributions
    ROUND(w.hurdle1_investor * t.percentage_of_investments, 2) AS investor_hurdle1,
    ROUND(w.hurdle2_investor * t.percentage_of_investments, 2) AS investor_hurdle2,
    ROUND(w.hurdle3_investor * t.percentage_of_investments, 2) AS investor_hurdle3,
    
    -- Residual Distribution
    ROUND(w.residual_investor * t.percentage_of_investments, 2) AS investor_residual,
    
    -- Total Distribution for this period
    ROUND(w.total_investor * t.percentage_of_investments, 2) AS investor_total

FROM {{ ref('tbl_waterfall_main') }} w
INNER JOIN hkh_dev.tbl_terms t ON w.portfolio_id = t.portfolio_id

-- Suppress periods where investor receives no distribution
WHERE (w.total_investor * t.percentage_of_investments) > 0

-- Order by investor and year for clear waterfall presentation
ORDER BY 
    t.investor_serial,
    w.year