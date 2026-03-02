-- Core: 10-min bucket KPIs (BPH chart, employee) - filtered to worked buckets
{{ config(materialized='view', tags=['core', 'marts']) }}

select *
from {{ ref('stg_shift_10min_kpi') }}
where minutes_worked_alloc > 0
