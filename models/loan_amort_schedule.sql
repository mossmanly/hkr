WITH recursive inputs AS (
  SELECT 
    property_id,
    purchase_price,
    ds_ltv,
    ds_term,
    ds_int,
    ds_refi_ltv,
    ds_refi_term,
    ds_refi_int,
    ds_refi_year,
    ROUND(purchase_price * ds_ltv, 0) AS original_loan_amount
  FROM inputs.property_inputs
),
years AS (
  SELECT generate_series(1, 30) AS year
),
base_schedule AS (
  SELECT 
    i.property_id,
    y.year,
    i.purchase_price,
    i.ds_refi_year,
    i.original_loan_amount,
    i.ds_ltv,
    i.ds_term,
    i.ds_int,
    i.ds_refi_ltv,
    i.ds_refi_term,
    i.ds_refi_int
  FROM inputs i 
  CROSS JOIN years y
),
loan_terms AS (
  SELECT 
    *,
    -- Use original terms before refi year, refi terms at/after refi year
    CASE 
      WHEN year < COALESCE(ds_refi_year, 999) THEN ds_ltv
      ELSE ds_refi_ltv
    END AS active_ltv,
    CASE 
      WHEN year < COALESCE(ds_refi_year, 999) THEN ds_term
      ELSE ds_refi_term
    END AS active_term,
    CASE 
      WHEN year < COALESCE(ds_refi_year, 999) THEN ds_int
      ELSE ds_refi_int
    END AS active_rate
  FROM base_schedule
),
payment_calcs AS (
  SELECT 
    *,
    -- Calculate payment based on active terms
    CASE 
      WHEN year < COALESCE(ds_refi_year, 999) THEN
        ROUND(
          original_loan_amount * 
          (active_rate * POWER(1 + active_rate, active_term)) /
          (POWER(1 + active_rate, active_term) - 1),
          0
        )
      ELSE
        ROUND(
          (purchase_price * active_ltv) * 
          (active_rate * POWER(1 + active_rate, active_term)) /
          (POWER(1 + active_rate, active_term) - 1),
          0
        )
    END AS annual_payment,
    -- Refi loan amount
    CASE 
      WHEN year = ds_refi_year THEN ROUND(purchase_price * active_ltv, 0)
      ELSE original_loan_amount
    END AS loan_amount_for_year
  FROM loan_terms
),
amort AS (
  -- Year 1
  SELECT 
    property_id,
    year,
    annual_payment,
    ROUND(original_loan_amount, 0) AS starting_balance,
    ROUND(original_loan_amount * active_rate, 0) AS interest_payment,
    ROUND(annual_payment - (original_loan_amount * active_rate), 0) AS principal_payment,
    ROUND(original_loan_amount - (annual_payment - (original_loan_amount * active_rate)), 0) AS ending_balance,
    active_rate,
    ds_refi_year,
    0::numeric AS refi_proceeds
  FROM payment_calcs
  WHERE year = 1

  UNION ALL

  SELECT 
    pc.property_id,
    pc.year,
    pc.annual_payment,
    -- Starting balance (handle refi transition)
    CASE 
      WHEN pc.year = pc.ds_refi_year THEN pc.loan_amount_for_year
      ELSE a.ending_balance
    END AS starting_balance,
    -- Interest payment
    CASE 
      WHEN pc.year = pc.ds_refi_year THEN ROUND(pc.loan_amount_for_year * pc.active_rate, 0)
      ELSE ROUND(a.ending_balance * pc.active_rate, 0)
    END AS interest_payment,
    -- Principal payment
    CASE 
      WHEN pc.year = pc.ds_refi_year THEN ROUND(pc.annual_payment - (pc.loan_amount_for_year * pc.active_rate), 0)
      ELSE ROUND(pc.annual_payment - (a.ending_balance * pc.active_rate), 0)
    END AS principal_payment,
    -- Ending balance
    CASE 
      WHEN pc.year = pc.ds_refi_year THEN 
        ROUND(pc.loan_amount_for_year - (pc.annual_payment - (pc.loan_amount_for_year * pc.active_rate)), 0)
      ELSE 
        ROUND(a.ending_balance - (pc.annual_payment - (a.ending_balance * pc.active_rate)), 0)
    END AS ending_balance,
    pc.active_rate,
    pc.ds_refi_year,
    -- Refi proceeds
    CASE 
      WHEN pc.year = pc.ds_refi_year THEN ROUND(pc.loan_amount_for_year - a.ending_balance, 0)::numeric
      ELSE 0::numeric
    END AS refi_proceeds
  FROM amort a
  JOIN payment_calcs pc ON pc.property_id = a.property_id AND pc.year = a.year + 1
  WHERE a.year < 30 
    AND (a.ending_balance > 0 OR pc.year = pc.ds_refi_year)
)
SELECT 
  property_id,
  year,
  annual_payment,
  starting_balance,
  interest_payment,
  principal_payment,
  ending_balance,
  active_rate,
  refi_proceeds,
  CASE WHEN year = ds_refi_year THEN 'REFI YEAR' ELSE '' END AS notes
FROM amort 
ORDER BY property_id, year