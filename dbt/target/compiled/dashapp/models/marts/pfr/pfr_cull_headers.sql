with __dbt__cte__stg_ptrun_cull_header as (
-- Staging: cull header - PTRUN_CULL_HEADER


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_CULL_HEADER
) -- PFR: QC temps, inspector (app filters by unique_run_key in (...))


select *
from __dbt__cte__stg_ptrun_cull_header