-- File: models/config/incentive_programs.sql (EXPANDED with Top 20)
-- Just replace your current file with this comprehensive version

{{ config(materialized='table') }}

SELECT * FROM (
  VALUES
    -- EXISTING PROGRAMS (Updated)
    ('ETO_OR_HEATPUMP', 'Energy Trust Heat Pump Rebate', 'hvac systems', 2000, 'fixed_amount', 
     NULL, 5000, 'portland_metro', '2025-01-01', '2030-12-31', 'utility', 'reduces_basis', 9, 0.75,
     45, '45_days', 'rebate_check', NULL, 'Apply before installation', 'per_system'),
     
    ('FED_SOLAR_ITC', 'Federal Solar Investment Tax Credit', 'solar', 0.30, 'percentage',
     NULL, NULL, 'national', '2025-01-01', '2025-12-31', 'federal', 'tax_credit', 9, 0.85,
     450, 'next_tax_season', 'tax_filing', 15, 'File Form 5695 with tax return', 'per_installation'),
     
    ('PORTLAND_SOLAR_REBATE', 'Portland Clean Energy Fund Solar Rebate', 'solar', 1000, 'fixed_amount',
     10000, 15000, 'portland', '2025-01-01', '2027-12-31', 'city', 'reduces_basis', 7, 0.60,
     90, '90_days', 'rebate_check', NULL, 'Limited funding - apply early', 'per_installation'),

    -- NEW HIGH-IMPACT PROGRAMS
    ('ODOE_HEAT_PUMP_2025', 'Oregon Heat Pump Purchase Program', 'hvac systems', 2000, 'fixed_amount',
     NULL, NULL, 'oregon_statewide', '2025-06-01', '2026-12-31', 'state', 'reduces_basis', 9, 0.80,
     30, '30_days', 'rebate_check', 14, 'NEW: Available for rental properties!', 'per_system'),

    ('FED_HEAT_PUMP_CREDIT', 'Federal Heat Pump Tax Credit', 'hvac systems', 0.30, 'percentage',
     2000, NULL, 'national', '2025-01-01', '2032-12-31', 'federal', 'tax_credit', 9, 0.85,
     450, 'next_tax_season', 'tax_filing', 0, 'Includes heat pump water heaters', 'per_system'),

    ('FED_ENERGY_EFFICIENCY', 'Federal Energy Efficiency Tax Credit', 'weatherization', 0.30, 'percentage',
     1200, 500, 'national', '2025-01-01', '2032-12-31', 'federal', 'tax_credit', 9, 0.80,
     450, 'next_tax_season', 'tax_filing', 0, 'Windows, insulation, doors qualify', 'per_project'),

    ('COMMERCIAL_179D', 'Commercial Building Energy Efficiency Deduction', 'energy efficiency', 5.00, 'per_sqft',
     NULL, 10000, 'national', '2025-01-01', '2032-12-31', 'federal', 'tax_deduction', 9, 0.90,
     365, 'next_tax_season', 'tax_filing', 60, 'Requires energy modeling certification', 'per_building'),

    ('ETO_WEATHERIZATION', 'Energy Trust Weatherization Package', 'weatherization', 0.25, 'percentage',
     1500, 300, 'portland_metro', '2025-01-01', '2026-12-31', 'utility', 'reduces_basis', 8, 0.75,
     60, '60_days', 'rebate_check', NULL, 'Insulation, air sealing, duct work', 'per_project'),

    ('PGE_HEAT_PUMP', 'PGE Heat Pump Rebate', 'hvac systems', 1500, 'fixed_amount',
     NULL, NULL, 'portland_metro', '2025-01-01', '2025-12-31', 'utility', 'reduces_basis', 7, 0.70,
     45, '45_days', 'rebate_check', 14, 'Must be PGE customer', 'per_system'),

    ('FED_BATTERY_CREDIT', 'Federal Battery Storage Tax Credit', 'battery storage', 0.30, 'percentage',
     NULL, 3, 'national', '2025-01-01', '2032-12-31', 'federal', 'tax_credit', 9, 0.85,
     450, 'next_tax_season', 'tax_filing', 0, 'Minimum 3 kWh capacity required', 'per_system'),

    ('ETO_SOLAR_BATTERY', 'Energy Trust Solar + Storage Rebate', 'battery storage', 2500, 'fixed_amount',
     NULL, 5, 'portland_metro', '2025-01-01', '2026-06-30', 'utility', 'reduces_basis', 6, 0.60,
     60, '60_days', 'rebate_check', 30, 'Limited funding available', 'per_system'),

    ('LOW_INCOME_HEAT_PUMP', 'Low Income Heat Pump Enhanced Rebate', 'hvac systems', 4000, 'fixed_amount',
     NULL, NULL, 'oregon_statewide', '2025-01-01', '2027-12-31', 'state', 'reduces_basis', 8, 0.85,
     45, '45_days', 'rebate_check', 21, 'Income qualification required', 'per_system'),

    ('EWEB_EFFICIENCY', 'EWEB Energy Efficiency Rebate', 'energy efficiency', 0.20, 'percentage',
     800, 200, 'eugene', '2025-01-01', '2025-12-31', 'utility', 'reduces_basis', 7, 0.70,
     30, '30_days', 'rebate_check', NULL, 'Eugene Water & Electric Board customers', 'per_project'),

    ('PACIFIC_POWER_HP', 'Pacific Power Heat Pump Incentive', 'hvac systems', 1200, 'fixed_amount',
     NULL, NULL, 'eastern_oregon', '2025-01-01', '2025-12-31', 'utility', 'reduces_basis', 7, 0.65,
     60, '60_days', 'rebate_check', 14, 'Eastern Oregon coverage', 'per_system'),

    ('MULTIFAMILY_SOLAR', 'Multifamily Solar Incentive Program', 'solar', 0.20, 'percentage',
     50000, 25000, 'oregon_statewide', '2025-01-01', '2026-12-31', 'state', 'reduces_basis', 8, 0.70,
     120, '120_days', 'rebate_check', 45, 'For properties with 5+ units', 'per_installation'),

    ('EV_CHARGING_REBATE', 'EV Charging Infrastructure Rebate', 'ev charging', 5000, 'fixed_amount',
     NULL, NULL, 'portland_metro', '2025-01-01', '2026-12-31', 'state', 'reduces_basis', 7, 0.75,
     60, '60_days', 'rebate_check', 30, 'Level 2 and DC fast charging', 'per_charger'),

    ('RURAL_ENERGY_AUDIT', 'Rural Energy Efficiency Program', 'energy efficiency', 0.50, 'percentage',
     2000, 500, 'rural_oregon', '2025-01-01', '2026-12-31', 'state', 'reduces_basis', 8, 0.80,
     45, '45_days', 'rebate_check', NULL, 'Non-urban areas, enhanced rates', 'per_project'),

    ('WATER_HEATER_HP', 'Heat Pump Water Heater Rebate', 'water heating', 1000, 'fixed_amount',
     NULL, NULL, 'portland_metro', '2025-01-01', '2026-12-31', 'utility', 'reduces_basis', 8, 0.75,
     45, '45_days', 'rebate_check', NULL, 'Energy Trust program', 'per_system'),

    ('WINDOW_UPGRADE', 'Energy Efficient Window Rebate', 'windows', 0.25, 'percentage',
     1000, 300, 'portland_metro', '2025-01-01', '2025-12-31', 'utility', 'reduces_basis', 7, 0.70,
     60, '60_days', 'rebate_check', NULL, 'ENERGY STAR certified windows', 'per_project'),

    ('DUCTLESS_MINI_SPLIT', 'Ductless Heat Pump Rebate', 'hvac systems', 1500, 'fixed_amount',
     NULL, NULL, 'portland_metro', '2025-01-01', '2025-12-31', 'utility', 'reduces_basis', 8, 0.75,
     45, '45_days', 'rebate_check', NULL, 'Per indoor unit installed', 'per_unit')

) AS programs(
  program_id, program_name, improvement_category, incentive_rate, incentive_structure,
  incentive_cap, minimum_project_size, market, effective_date, expiration_date,
  jurisdiction_level, tax_treatment, funding_stability_score, capture_rate_assumption,
  cash_timing_days, cash_timing_period, cash_timing_type, application_lead_time_days,
  tips_for_success, incentive_unit
)