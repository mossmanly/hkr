-- models/refi_outcomes.sql

WITH refi_inputs AS (
    SELECT 
        property_id,
        purchase_price,
        ds_refi_year,
        ds_refi_ltv,
        ds_refi_term,
        ds_refi_int,
        ROUND(purchase_price * POWER(1.03, ds_refi_year), 2) AS estimated_value_at_refi
    FROM {{ source('inputs', 'property_inputs') }}
),

balance_at_refi AS (
    SELECT 
        property_id,
        year,
        ending_balance AS original_loan_balance
    FROM {{ ref('loan_amort_schedule') }}
),

joined AS (
    SELECT 
        ri.property_id,
        ri.estimated_value_at_refi,
        ri.ds_refi_year,
        ri.ds_refi_ltv,
        ri.ds_refi_term,
        ri.ds_refi_int,
        br.original_loan_balance
    FROM refi_inputs ri
    LEFT JOIN balance_at_refi br 
        ON ri.property_id = br.property_id 
       AND br.year = ri.ds_refi_year - 1
),

calculated AS (
    SELECT
        property_id,
        estimated_value_at_refi,
        original_loan_balance,
        ROUND(estimated_value_at_refi * ds_refi_ltv, 2) AS refi_loan_amount,
        ROUND((estimated_value_at_refi * ds_refi_ltv) - original_loan_balance, 2) AS refi_proceeds,
        ROUND(
            (estimated_value_at_refi * ds_refi_ltv)
            * (ds_refi_int * POWER(1 + ds_refi_int, ds_refi_term))
            / (POWER(1 + ds_refi_int, ds_refi_term) - 1),
            2
        ) AS refi_annual_ds,
        ds_refi_year
    FROM joined
)

SELECT * FROM calculated