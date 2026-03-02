-- PFR: cull defect counts (app filters by unique_run_key in (...))
{{ config(materialized='view', tags=['pfr', 'marts']) }}

select *
from {{ ref('stg_ptrun_cull_defect') }}
