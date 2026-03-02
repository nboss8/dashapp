-- Core: run keys for slicers (grower_number, run_key, packdate_run_key, day_label)
{{ config(materialized='view', tags=['core', 'marts']) }}

select grower_number, run_key, packdate_run_key, day_label, date_d
from {{ ref('stg_ptrun_report') }}
