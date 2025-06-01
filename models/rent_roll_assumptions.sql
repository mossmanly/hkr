-- models/rent_roll_detailed.sql

with years as (
    select * from {{ ref('dim_years') }}
),
unit_rents as (
    select * from inputs.unit_mix_rents
),
vacancy as (
    select property_id, vacancy_rate from inputs.rent_roll_assumptions
),
expanded as (
    select 
        r.property_id,
        r.unit_type,
        r.unit_count,
        r.monthly_rent,
        y.year,
        v.vacancy_rate
    from unit_rents r
    join vacancy v on r.property_id = v.property_id
    cross join years y
),
annualized as (
    select 
        property_id,
        year,
        sum(unit_count * monthly_rent * 12 * (1 - vacancy_rate)) as annual_rent
    from expanded
    group by property_id, year
)

select * from annualized