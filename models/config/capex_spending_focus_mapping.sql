-- File: models/config/capex_spending_focus_mapping.sql
{{ config(materialized='table') }}

SELECT 
  spending_focus,
  improvement_categories,
  typical_project_examples,
  sustainability_opportunities
FROM (
  VALUES
    -- High-Impact Sustainability Focus Areas
    ('Energy Efficiency', 
     ARRAY['hvac systems', 'energy efficiency', 'weatherization', 'windows', 'water heating'],
     'HVAC system upgrades, smart thermostats, insulation improvements, LED lighting conversion, Energy Star appliances',
     'Excellent incentive opportunities - utilities and government programs heavily support energy efficiency'),
     
    ('HVAC Systems + Maintenance',
     ARRAY['hvac systems', 'water heating'],
     'Heat pump installations, high-efficiency furnaces, ductwork sealing, smart controls',
     'Heat pumps and high-efficiency systems qualify for federal tax credits and utility rebates'),
     
    ('Windows + Weatherization',
     ARRAY['weatherization', 'windows', 'energy efficiency'],
     'Energy-efficient windows, weather stripping, caulking, storm doors, insulation',
     'Window upgrades often qualify for energy efficiency rebates and tax credits'),
     
    ('Roofing + Final Snap Renos',
     ARRAY['solar', 'battery storage'],
     'Roof replacement, solar-ready infrastructure, attic insulation, ventilation improvements',
     'Solar-ready roofing and cool roofs may qualify for green building incentives'),
     
    -- Moderate Sustainability Potential
    ('Major Systems',
     ARRAY['hvac systems', 'water heating', 'energy efficiency'],
     'Electrical panel upgrades, plumbing improvements, water heater replacement',
     'Water heater upgrades (heat pump units) and electrical for EV charging may qualify'),
     
    ('Exterior + Structural',
     ARRAY['weatherization', 'ev charging'],
     'Siding replacement, foundation repairs, landscaping, exterior painting',
     'Limited incentive opportunities - focus on green materials and water-efficient landscaping'),
     
    ('Flooring + Interior',
     ARRAY[]::TEXT[],
     'Flooring replacement, interior painting, fixture updates, cabinetry',
     'Limited incentives - look for green/low-VOC materials certifications'),
     
    -- Lower Sustainability Priority (but still important)
    ('Snap Renos + Emergency Repairs',
     ARRAY[]::TEXT[],
     'Emergency fixes, safety improvements, quick cosmetic updates',
     'Limited incentive opportunities - focus on safety and code compliance'),
     
    ('Snap Renos + Maintenance',
     ARRAY[]::TEXT[],
     'Routine maintenance, touch-up painting, minor fixture replacements',
     'Minimal incentive opportunities - consider efficiency upgrades when possible'),
     
    ('Preventive Maintenance',
     ARRAY[]::TEXT[],
     'Regular inspections, filter changes, minor repairs, preventive care',
     'Generally not eligible for incentives - focus on maintaining efficient systems')
     
) AS mapping(spending_focus, improvement_categories, typical_project_examples, sustainability_opportunities)