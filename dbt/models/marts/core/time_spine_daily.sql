-- Time spine for MetricFlow: daily granularity
-- Required for time-based joins and aggregations in the Semantic Layer
{{ config(
    materialized='table',
    tags=['core', 'marts', 'time_spine'],
) }}

with base_dates as (
    select dateadd(day, row_number() over (order by 1) - 1, '2000-01-01'::date) as date_day
    from table(generator(rowcount => 11000))
),
final as (
    select cast(date_day as date) as date_day
    from base_dates
    where date_day >= dateadd(year, -5, current_date())
      and date_day <= dateadd(day, 30, current_date())
)
select * from final
