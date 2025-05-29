{{ config(materialized = 'view') }}

select
  1 as id,
  'hello dbt' as message
