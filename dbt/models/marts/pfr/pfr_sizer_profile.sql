-- PFR: sizer drop + packed join (app filters by unique_run_key in (...) and aggregates)
{{ config(materialized='view', tags=['pfr', 'marts']) }}

select
    d.UNIQUE_RUN_KEY,
    d.GRADE_NAME,
    d.SIZE_NAME,
    d.WEIGHT_DEC
from {{ ref('stg_ptrun_sizer_drop') }} d
inner join {{ ref('stg_ptrun_sizer_packed') }} p
    on d.UNIQUE_RUN_KEY = p.UNIQUE_RUN_KEY
    and d.QUALITY_NAME = p.QUALITY_NAME
    and d.GRADE_NAME = p.GRADE_NAME
    and d.SIZE_NAME = p.SIZE_NAME
where p.IS_PACKED = true
