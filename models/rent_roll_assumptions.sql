{{ config(materialized="view") }}

with rent_roll as (
  select * from {{ ref('rent_roll') }}
)

select
  company_id,
  portfolio_id,
  property_id,
  acquisition_year,
  unit_count,
  avg_rent_per_unit as avg_rent,
  turnover_rate,
  turnover_rate_bump,
  rent_growth_rate
from rent_roll