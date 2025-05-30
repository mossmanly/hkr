{{ config(materialized="view") }}

with rent_roll as (
  select * from {{ ref('rent_roll') }}
)

select
  company_id,
  portfolio_id,
  property_id,
  acquisition_year,
  unit_count        as unit_number,
  avg_rent_per_unit as rent_amount,
  turnover_rate,
  turnover_rate_bump,
  rent_growth_rate
from rent_roll