{{ config(materialized='view') }}

select
  portfolio_id
from {{ source('hkh_dev', 'tbl_terms') }}
limit 10