
- models/invested_metrics.sql
{{ config (materialized='view') }}
-- Calculate the percentage of each equity investment as a sum of total investment
SELECT
    equity_invested,
    equity_invested / SELECT SUM(equity_invested) *100 AS percentage_of_investments
    FROM {{'hkh_dev','tbl_terms'}} 
group by {{hkh_dev.investor_serial}}
