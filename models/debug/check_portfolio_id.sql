{{ config(materialized='view') }}

with atcf_by_portfolio as (
    select
        portfolio_id,
        year,
        sum(atcf) as distributable_cash
    from {{ ref('fact_property_cash_flow') }}
    group by portfolio_id, year
),

terms_by_class as (
    select
        portfolio_id,
        equity_class,
        sum(equity_contributed) as total_equity
    from {{ source('hkh_dev', 'tbl_terms') }}
    group by portfolio_id, equity_class
)

select
    a.portfolio_id,
    a.year,
    a.distributable_cash,
    coalesce(p.total_equity, 0) as preferred_equity,
    coalesce(c.total_equity, 0) as common_equity
from atcf_by_portfolio a
left join terms_by_class p on a.portfolio_id = p.portfolio_id and p.equity_class = 'Preferred'
left join terms_by_class c on a.portfolio_id = c.portfolio_id and c.equity_class = 'Common'
limit 10