{{ config(materialized="view") }}

with rent_roll as (
  select *
  from {{ source('raw', 'rent_roll') }}
)

select
  property_id,
  unit_number,
  rent_amount,
  square_feet,
  rent_amount / nullif(square_feet, 0) as rent_per_sqft
from rent_roll