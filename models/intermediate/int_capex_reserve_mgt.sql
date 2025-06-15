-- models/capex_reserve_mgt.sql
-- Core capex reserve management with period-by-period factor application
-- Applies capex ratios by property/year with projected spending patterns

{{ config(materialized='table') }}

WITH property_basics AS (
    SELECT 
        property_id,
        unit_count,
        capex_per_unit,
        purchase_price,
        gross_annual_income
    FROM hkh_dev.stg_property_inputs
    WHERE property_id IN ('P1', 'P2', 'P3', 'P5')  -- Valid properties only
),

capex_factors_with_rebates AS (
    SELECT 
        property_id,
        year,
        capex_factor,
        spending_focus,
        rationale,
        heat_pump_rebate_per_unit,
        efficiency_rebate_per_unit,
        solar_rebate_per_unit
    FROM hkh_dev.stg_capex_factors
    WHERE property_id IN ('P1', 'P2', 'P3', 'P5')
),

-- Apply capex factors by property by period to get annual capex amounts
annual_capex_by_period AS (
    SELECT 
        pb.property_id,
        cf.year,
        cf.spending_focus,
        cf.rationale,
        
        -- Base calculations
        pb.unit_count,
        pb.capex_per_unit,
        pb.purchase_price,
        pb.gross_annual_income,
        
        -- Factor application - this is where the magic happens
        cf.capex_factor,
        ROUND(pb.unit_count * pb.capex_per_unit * cf.capex_factor, 0) AS annual_capex_budget,
        
        -- Rebate potential per unit
        cf.heat_pump_rebate_per_unit,
        cf.efficiency_rebate_per_unit,
        cf.solar_rebate_per_unit,
        
        -- Total rebate potential for this property/year
        ROUND(pb.unit_count * (
            COALESCE(cf.heat_pump_rebate_per_unit, 0) + 
            COALESCE(cf.efficiency_rebate_per_unit, 0) + 
            COALESCE(cf.solar_rebate_per_unit, 0)
        ), 0) AS total_rebate_potential,
        
        -- Business context
        ROUND(pb.unit_count * pb.capex_per_unit * cf.capex_factor / pb.gross_annual_income * 100, 2) AS capex_as_percent_income

    FROM property_basics pb
    INNER JOIN capex_factors_with_rebates cf ON pb.property_id = cf.property_id
),

-- Project actual capex spending patterns (the "wild ass guesses")
capex_spending_projections AS (
    SELECT 
        acp.*,
        
        -- Projected actual spending based on spending focus and timing patterns
        CASE 
            -- Turn renovations - big spending years
            WHEN acp.spending_focus = 'Roofing + Final Snap Renos' THEN acp.annual_capex_budget * 1.2
            WHEN acp.spending_focus = 'Flooring + Interior' THEN acp.annual_capex_budget * 1.1
            WHEN acp.spending_focus = 'Exterior + Structural' THEN acp.annual_capex_budget * 1.3
            
            -- Major systems - cyclical heavy spending
            WHEN acp.spending_focus = 'HVAC Systems + Maintenance' AND acp.year % 5 = 0 THEN acp.annual_capex_budget * 2.0
            WHEN acp.spending_focus = 'Major Systems' AND acp.year % 6 = 0 THEN acp.annual_capex_budget * 1.8
            
            -- Energy efficiency - moderate consistent spending
            WHEN acp.spending_focus = 'Energy Efficiency' THEN acp.annual_capex_budget * 0.9
            WHEN acp.spending_focus = 'Windows + Weatherization' THEN acp.annual_capex_budget * 1.0
            
            -- Maintenance and emergency - steady lower spending
            WHEN acp.spending_focus = 'Snap Renos + Emergency Repairs' THEN acp.annual_capex_budget * 0.7
            WHEN acp.spending_focus = 'Snap Renos + Maintenance' THEN acp.annual_capex_budget * 0.6
            WHEN acp.spending_focus = 'Preventive Maintenance' THEN acp.annual_capex_budget * 0.5
            
            -- Default case
            ELSE acp.annual_capex_budget
        END AS projected_capex_spent,
        
        -- Reserve accumulation pattern (what we set aside vs what we spend)
        CASE 
            WHEN acp.spending_focus IN ('Snap Renos + Emergency Repairs', 'Snap Renos + Maintenance', 'Preventive Maintenance') 
                THEN acp.annual_capex_budget  -- Spend as we reserve
            ELSE acp.annual_capex_budget * 1.1  -- Reserve slightly more than budget for big projects
        END AS annual_reserves_set_aside

    FROM annual_capex_by_period acp
),

-- Calculate running balances and cash management
capex_cash_flows AS (
    SELECT 
        csp.*,
        
        -- Running reserve balance 
        SUM(csp.annual_reserves_set_aside) OVER (
            PARTITION BY csp.property_id 
            ORDER BY csp.year 
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_reserves_raised,
        
        -- Running spending total
        SUM(csp.projected_capex_spent) OVER (
            PARTITION BY csp.property_id 
            ORDER BY csp.year 
            ROWS UNBOUNDED PRECEDING
        ) AS cumulative_capex_spent,
        
        -- Available cash balance
        SUM(csp.annual_reserves_set_aside) OVER (
            PARTITION BY csp.property_id 
            ORDER BY csp.year 
            ROWS UNBOUNDED PRECEDING
        ) - SUM(csp.projected_capex_spent) OVER (
            PARTITION BY csp.property_id 
            ORDER BY csp.year 
            ROWS UNBOUNDED PRECEDING
        ) AS available_for_capex,
        
        -- Add interest earned on reserves (2% annually) - this flows to cash flow!
        ROUND(SUM(csp.annual_reserves_set_aside) OVER (
            PARTITION BY csp.property_id 
            ORDER BY csp.year 
            ROWS UNBOUNDED PRECEDING
        ) * 0.02, 0) AS annual_interest_on_reserves,
        
        ROUND(SUM(csp.annual_reserves_set_aside) OVER (
            PARTITION BY csp.property_id 
            ORDER BY csp.year 
            ROWS UNBOUNDED PRECEDING
        ) * 1.02, 0) AS total_reserves_raised

    FROM capex_spending_projections csp
),

-- Final output with business classifications
final_capex_management AS (
    SELECT 
        ccf.*,
        
        -- Performance metrics
        ROUND(ccf.projected_capex_spent / NULLIF(ccf.annual_capex_budget, 0), 2) AS spending_vs_budget_ratio,
        ROUND(ccf.available_for_capex / NULLIF(ccf.annual_reserves_set_aside, 0), 1) AS months_of_runway,
        
        -- Cash position analysis
        CASE 
            WHEN ccf.available_for_capex < 0 THEN 'Cash Deficit'
            WHEN ccf.available_for_capex < ccf.annual_reserves_set_aside THEN 'Low Cash'
            WHEN ccf.available_for_capex > ccf.annual_reserves_set_aside * 3 THEN 'Excess Cash'
            ELSE 'Balanced'
        END AS cash_position_status,
        
        -- Spending pattern classification
        CASE 
            WHEN ccf.spending_focus IN ('Roofing + Final Snap Renos', 'Flooring + Interior', 'Exterior + Structural') 
                THEN 'Turn Renovation'
            WHEN ccf.spending_focus IN ('HVAC Systems + Maintenance', 'Major Systems') 
                THEN 'Major Systems'
            WHEN ccf.spending_focus IN ('Energy Efficiency', 'Windows + Weatherization') 
                THEN 'Energy Efficiency'
            WHEN ccf.spending_focus LIKE '%Maintenance%' OR ccf.spending_focus LIKE '%Emergency%'
                THEN 'Maintenance & Emergency'
            ELSE 'Other'
        END AS capex_category,
        
        -- Rebate opportunity classification
        CASE 
            WHEN ccf.total_rebate_potential > ccf.projected_capex_spent * 0.3 THEN 'High Rebate Potential'
            WHEN ccf.total_rebate_potential > ccf.projected_capex_spent * 0.1 THEN 'Moderate Rebate Potential'
            WHEN ccf.total_rebate_potential > 0 THEN 'Low Rebate Potential'
            ELSE 'No Rebates Available'
        END AS rebate_opportunity_tier

    FROM capex_cash_flows ccf
)

SELECT 
    -- Core identifiers
    property_id,
    year,
    spending_focus,
    rationale,
    
    -- Property fundamentals
    unit_count,
    capex_per_unit,
    purchase_price,
    gross_annual_income,
    capex_as_percent_income,
    
    -- Factor application and budgeting
    capex_factor,
    annual_capex_budget,
    annual_reserves_set_aside,
    projected_capex_spent AS capex_spent,  -- This is what fact_capex_with_incentives expects
    spending_vs_budget_ratio,
    
    -- Cash flow management
    cumulative_reserves_raised,
    total_reserves_raised,
    cumulative_capex_spent,
    available_for_capex,
    annual_interest_on_reserves,  -- THIS flows into BTCF→ATCF calculations!
    months_of_runway,
    cash_position_status,
    
    -- Rebate analysis
    heat_pump_rebate_per_unit,
    efficiency_rebate_per_unit,
    solar_rebate_per_unit,
    total_rebate_potential,
    rebate_opportunity_tier,
    
    -- Business classifications
    capex_category,
    
    -- Performance ratios for marts
    ROUND(total_reserves_raised / NULLIF(purchase_price, 0) * 100, 2) AS reserves_as_percent_value,
    ROUND(cumulative_capex_spent / NULLIF(purchase_price, 0) * 100, 2) AS capex_spent_as_percent_value

FROM final_capex_management

ORDER BY property_id, year