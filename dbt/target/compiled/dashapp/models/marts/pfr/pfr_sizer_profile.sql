with __dbt__cte__stg_ptrun_sizer_drop as (
-- Staging: sizer drop snapshot - PTRUN_SIZER_DROP_SNAPSHOT


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_SIZER_DROP_SNAPSHOT
),  __dbt__cte__stg_ptrun_sizer_packed as (
-- Staging: sizer packed - PTRUN_SIZER_PACKED


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_SIZER_PACKED
) -- PFR: sizer drop + packed join (app filters by unique_run_key in (...) and aggregates)


select
    d.UNIQUE_RUN_KEY,
    d.GRADE_NAME,
    d.SIZE_NAME,
    d.WEIGHT_DEC
from __dbt__cte__stg_ptrun_sizer_drop d
inner join __dbt__cte__stg_ptrun_sizer_packed p
    on d.UNIQUE_RUN_KEY = p.UNIQUE_RUN_KEY
    and d.QUALITY_NAME = p.QUALITY_NAME
    and d.GRADE_NAME = p.GRADE_NAME
    and d.SIZE_NAME = p.SIZE_NAME
where p.IS_PACKED = true