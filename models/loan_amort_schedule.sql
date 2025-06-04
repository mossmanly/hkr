WITH recursive inputs AS (
  SELECT 
    property_id,
    purchase_price,
    ds_ltv,
    ds_term,
    ds_int,
    ROUND(purchase_price * ds_ltv, 2) AS loan_amount
  FROM inputs.property_inputs
),
years AS (
  SELECT generate_series(1, 30) AS year
),
schedule AS (
  SELECT 
    i.property_id,
    y.year,
    ROUND(
      i.loan_amount * 
      (i.ds_int * POWER(1 + i.ds_int, i.ds_term)) /
      (POWER(1 + i.ds_int, i.ds_term) - 1),
      2
    ) AS annual_payment,
    i.ds_int,
    i.ds_term,
    i.loan_amount
  FROM inputs i CROSS JOIN years y
),
amort AS (
  SELECT 
    property_id,
    year,
    annual_payment,
    ROUND(loan_amount, 2) AS starting_balance,
    ROUND(loan_amount * ds_int, 2) AS interest_payment,
    ROUND(annual_payment - (loan_amount * ds_int), 2) AS principal_payment,
    ROUND(loan_amount - (annual_payment - (loan_amount * ds_int)), 2) AS ending_balance
  FROM schedule
  WHERE year = 1

  UNION ALL

  SELECT 
    s.property_id,
    s.year,
    s.annual_payment,
    a.ending_balance,
    ROUND(a.ending_balance * s.ds_int, 2),
    ROUND(s.annual_payment - (a.ending_balance * s.ds_int), 2),
    ROUND(a.ending_balance - (s.annual_payment - (a.ending_balance * s.ds_int)), 2)
  FROM amort a
  JOIN schedule s 
    ON s.property_id = a.property_id AND s.year = a.year + 1
  WHERE a.year < s.ds_term
)
SELECT * FROM amort