-- models/rent_roll_detailed.sql

with years as (
    select * from {{ ref('dim_years') }}
),
unit_rents as (
    select * from inputs.unit_mix_rents
),
assumptions as (
    select 
        property_id, 
        vacancy_rate, 
        cola_snap,
        norm_turn_rate,
        reno_snap,
        norm_snap,
        ltl_days_reno,
        ltl_days_norm
    from inputs.rent_roll_assumptions
),
expanded as (
    select 
        r.property_id,
        r.unit_type,
        r.unit_count,
        r.monthly_rent,
        y.year,
        y.year_offset,
        a.vacancy_rate,
        a.cola_snap,
        a.norm_turn_rate,
        a.reno_snap,
        a.norm_snap,
        a.ltl_days_reno,
        a.ltl_days_norm,

        floor(norm_turn_rate * y.year_offset) as turns_to_date,

        -- LTL as percentages
        (a.ltl_days_reno / 365.0) as ltl_reno_pct,
        (a.ltl_days_norm / 365.0) as ltl_norm_pct
    from unit_rents r
    join assumptions a on r.property_id = a.property_id
    cross join years y
),
annualized as (
    select 
        property_id,
        year,
        sum(
            unit_count 
            * monthly_rent 
            * 12 
            * (1 - vacancy_rate)
            * pow(1 + cola_snap, year_offset)
            * (
                case 
                    when turns_to_date = 0 then 1.0
                    when turns_to_date = 1 then 
                        (1 + reno_snap) * (1 - ltl_reno_pct)
                    else 
                        (1 + reno_snap) * pow(1 + norm_snap, turns_to_date - 1) * (1 - ltl_norm_pct)
                end
            )
        ) as annual_rent
    from expanded
    group by property_id, year
)

select * from annualized