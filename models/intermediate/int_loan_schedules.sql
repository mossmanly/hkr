{{
  config(
    materialized='table',
    indexes=[
      {'columns': ['property_id'], 'unique': false},
      {'columns': ['portfolio_id'], 'unique': false},
      {'columns': ['payment_number'], 'unique': false}
    ]
  )
}}

-- Extract loan amortization logic from loan_amort_schedule
-- Creates detailed loan payment schedules for debt service analysis
-- References only staging models for clean data lineage

WITH base_properties AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    property_name,
    purchase_price,
    ds_ltv,
    ds_int,
    ds_term
  FROM {{ ref('stg_property_inputs') }}
  WHERE company_id = 1 
    AND portfolio_id = 'micro-1'
    AND ds_ltv > 0 
    AND ds_int > 0 
    AND ds_term > 0
),

-- Generate payment numbers (months) for each loan
payment_periods AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    property_name,
    purchase_price,
    ds_ltv,
    ds_int,
    ds_term,
    purchase_price * ds_ltv AS loan_amount,
    
    -- Calculate monthly payment using standard mortgage formula
    (purchase_price * ds_ltv) * 
    ((ds_int/12) * POWER(1 + ds_int/12, ds_term * 12)) / 
    (POWER(1 + ds_int/12, ds_term * 12) - 1) AS monthly_payment,
    
    -- Generate payment number sequence (1 to total months)
    payment_number
  FROM base_properties
  CROSS JOIN (
    SELECT GENERATE_SERIES(1, 360) AS payment_number -- Max 30 years (PostgreSQL function)
  ) payment_seq
  WHERE payment_number <= (ds_term * 12)
),

-- Calculate amortization schedule
loan_schedule AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    property_name,
    purchase_price,
    loan_amount,
    ds_ltv,
    ds_int,
    ds_term,
    payment_number,
    monthly_payment,
    
    -- Beginning balance calculation
    -- For payment n: Balance = P * [(1+r)^total_payments - (1+r)^(n-1)] / [(1+r)^total_payments - 1]
    loan_amount * (
      (POWER(1 + ds_int/12, ds_term * 12) - POWER(1 + ds_int/12, payment_number - 1)) /
      (POWER(1 + ds_int/12, ds_term * 12) - 1)
    ) AS beginning_balance,
    
    -- Interest payment for this period
    (loan_amount * (
      (POWER(1 + ds_int/12, ds_term * 12) - POWER(1 + ds_int/12, payment_number - 1)) /
      (POWER(1 + ds_int/12, ds_term * 12) - 1)
    )) * (ds_int/12) AS interest_payment,
    
    -- Principal payment (total payment - interest)
    monthly_payment - ((loan_amount * (
      (POWER(1 + ds_int/12, ds_term * 12) - POWER(1 + ds_int/12, payment_number - 1)) /
      (POWER(1 + ds_int/12, ds_term * 12) - 1)
    )) * (ds_int/12)) AS principal_payment,
    
    -- Ending balance
    loan_amount * (
      (POWER(1 + ds_int/12, ds_term * 12) - POWER(1 + ds_int/12, payment_number)) /
      (POWER(1 + ds_int/12, ds_term * 12) - 1)
    ) AS ending_balance,
    
    -- Cumulative totals
    payment_number * monthly_payment AS cumulative_payments,
    
    -- Payment year for grouping
    CEIL(payment_number / 12.0) AS payment_year,
    
    -- Payment month within year
    CASE 
      WHEN payment_number % 12 = 0 THEN 12
      ELSE payment_number % 12
    END AS payment_month,
    
    -- Loan-to-value at this point in time
    CASE 
      WHEN purchase_price > 0 THEN
        (loan_amount * (
          (POWER(1 + ds_int/12, ds_term * 12) - POWER(1 + ds_int/12, payment_number)) /
          (POWER(1 + ds_int/12, ds_term * 12) - 1)
        )) / purchase_price
      ELSE 0
    END AS current_ltv,
    
    -- Percentage of loan paid off
    CASE 
      WHEN loan_amount > 0 THEN
        1 - ((loan_amount * (
          (POWER(1 + ds_int/12, ds_term * 12) - POWER(1 + ds_int/12, payment_number)) /
          (POWER(1 + ds_int/12, ds_term * 12) - 1)
        )) / loan_amount)
      ELSE 0
    END AS percent_paid_off,
    
    -- Flags for key periods
    CASE WHEN payment_number <= 12 THEN TRUE ELSE FALSE END AS is_first_year,
    CASE WHEN payment_number >= (ds_term * 12 - 11) THEN TRUE ELSE FALSE END AS is_final_year,
    CASE WHEN payment_number % 12 = 0 THEN TRUE ELSE FALSE END AS is_year_end,
    
    -- Metadata
    CURRENT_TIMESTAMP AS calculated_at,
    'int_loan_schedules' AS model_source
    
  FROM payment_periods
),

-- Add summary statistics for each loan
loan_summary_stats AS (
  SELECT 
    property_id,
    company_id,
    portfolio_id,
    
    -- Total payments over life of loan
    SUM(monthly_payment) AS total_payments_over_life,
    SUM(interest_payment) AS total_interest_over_life,
    SUM(principal_payment) AS total_principal_over_life,
    
    -- First year totals
    SUM(CASE WHEN is_first_year THEN monthly_payment ELSE 0 END) AS first_year_payments,
    SUM(CASE WHEN is_first_year THEN interest_payment ELSE 0 END) AS first_year_interest,
    SUM(CASE WHEN is_first_year THEN principal_payment ELSE 0 END) AS first_year_principal,
    
    -- Averages
    AVG(monthly_payment) AS avg_monthly_payment,
    AVG(interest_payment) AS avg_monthly_interest,
    AVG(principal_payment) AS avg_monthly_principal
    
  FROM loan_schedule
  GROUP BY property_id, company_id, portfolio_id
)

-- Return detailed schedule with summary stats
SELECT 
  ls.*,
  lss.total_payments_over_life,
  lss.total_interest_over_life,
  lss.total_principal_over_life,
  lss.first_year_payments,
  lss.first_year_interest,
  lss.first_year_principal
FROM loan_schedule ls
LEFT JOIN loan_summary_stats lss ON ls.property_id = lss.property_id
WHERE ls.company_id = 1 -- Preserve company scoping
  AND ls.portfolio_id = 'micro-1' -- Preserve portfolio filtering
ORDER BY ls.property_id, ls.payment_number