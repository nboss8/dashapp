with __dbt__cte__stg_ptrun_cull_defect as (
-- Staging: cull defect - PTRUN_CULL_DEFECT


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_CULL_DEFECT
) -- PFR: cull defect counts (app filters by unique_run_key in (...))


select *
from __dbt__cte__stg_ptrun_cull_defect