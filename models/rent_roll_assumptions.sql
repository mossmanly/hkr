{{ config(materialized="view") }}

with rent_roll as (            -- 0 spaces before “with”
  select *                     -- 2 spaces before “select”
  from {{ ref('rent_roll') }} -- still 2 spaces
)                              -- 0 spaces before “)”

select                         -- 0 spaces before “select”
  property_id,                 -- 2 spaces
  unit_number,                 -- 2 spaces
  rent_amount,                 -- 2 spaces
  square_feet,                 -- 2 spaces
  rent_amount / nullif(        -- 2 spaces
    square_feet, 0            -- 4 spaces inside nullif
  ) as rent_per_sqft          -- 2 spaces closing nullif
from rent_roll                -- 0 spaces