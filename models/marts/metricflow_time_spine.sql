{{ config(materialized='table') }}

with days as (
    {{ dbt_utils.date_spine('day', "cast('2000-01-01' as date)", "cast('2030-01-01' as date)") }}
)

select cast(date_day as date) as date_day
from days
