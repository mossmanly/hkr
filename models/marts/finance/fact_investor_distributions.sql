-- marts/finance/fact_investor_distributions.sql
-- Investor distributions by investor and year
-- Suppresses periods with zero distributions
-- Portfolio filtering with company scoping preserved
-- ROUNDED TO NO DECIMALS

{{ config(materialized='table') }}

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
    t.equity_class,
    t.percentage_of_investments,
    
    -- Individual Investor Distribution Calculations Based on Equity Class
    -- Preferred investors get preferred distributions
    CASE WHEN t.equity_class = 'Preferred' THEN
        ROUND((w.pref_roc_paid + w.common_roc_paid) * t.percentage_of_investments, 0)
    ELSE 0 END AS investor_roc,
    
    -- Preferred IRR (only for Preferred investors)
    CASE WHEN t.equity_class = 'Preferred' THEN
        ROUND(w.pref_irr_paid * t.percentage_of_investments, 0)
    ELSE 0 END AS investor_pref_irr,
    
    -- Common ROC and IRR (only for Common investors)
    CASE WHEN t.equity_class = 'Common' THEN
        ROUND((w.pref_roc_paid + w.common_roc_paid) * t.percentage_of_investments, 0)
    ELSE 0 END AS investor_common_roc,
    
    CASE WHEN t.equity_class = 'Common' THEN
        ROUND(w.common_irr_paid * t.percentage_of_investments, 0)
    ELSE 0 END AS investor_common_irr,
    
    -- Hurdle Distributions (allocated to all investors based on percentage)
    ROUND(w.hurdle1_investor * t.percentage_of_investments, 0) AS investor_hurdle1,
    ROUND(w.hurdle2_investor * t.percentage_of_investments, 0) AS investor_hurdle2,
    ROUND(w.hurdle3_investor * t.percentage_of_investments, 0) AS investor_hurdle3,
    
    -- Residual Distribution (allocated to all investors based on percentage)
    ROUND(w.residual_investor * t.percentage_of_investments, 0) AS investor_residual,
    
    -- Total Distribution for this investor this period
    ROUND(
        CASE WHEN t.equity_class = 'Preferred' THEN
            (w.pref_roc_paid + w.common_roc_paid + w.pref_irr_paid) * t.percentage_of_investments
        WHEN t.equity_class = 'Common' THEN
            (w.pref_roc_paid + w.common_roc_paid + w.common_irr_paid) * t.percentage_of_investments
        ELSE 0 END +
        (w.hurdle1_investor + w.hurdle2_investor + w.hurdle3_investor + w.residual_investor) * t.percentage_of_investments, 0
    ) AS investor_total

FROM {{ ref('fact_portfolio_waterfall') }} w
INNER JOIN {{ source('hkh_dev', 'tbl_terms') }} t ON w.portfolio_id = t.portfolio_id

-- Portfolio filtering: Only include investors for default portfolio of this company
INNER JOIN {{ source('inputs', 'portfolio_settings') }} ps 
    ON t.portfolio_id = ps.portfolio_id

WHERE ps.company_id = 1  -- Company scoping for future multi-tenancy
  AND ps.is_default = TRUE  -- Only include default portfolio
  -- Suppress periods where investor receives no distribution
  AND (w.total_investor * t.percentage_of_investments) > 0

-- Order by investor and year for clear waterfall presentation
ORDER BY 
    t.investor_serial ASC,
    w.year ASC