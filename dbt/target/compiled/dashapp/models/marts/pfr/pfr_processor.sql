with __dbt__cte__stg_ptrun_processor as (
-- Staging: processor view - PTRUN_PROCESSOR_VIEW_PBIX


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_PROCESSOR_VIEW_PBIX
) -- PFR: processor net weight by size (app filters by unique_run_key in (...))


select *
from __dbt__cte__stg_ptrun_processor