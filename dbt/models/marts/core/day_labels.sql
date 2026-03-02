-- Core: distinct day labels for dropdown (TODAY first)
{{ config(materialized='view', tags=['core', 'marts']) }}

select distinct day_label
from {{ ref('stg_ptrun_report') }}
order by case when day_label = 'TODAY' then 0 else 1 end, day_label
