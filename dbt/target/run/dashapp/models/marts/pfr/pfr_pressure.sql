
  create or replace   view FROSTY.DBT_DEV_DBT_DEV.pfr_pressure
  
  
  
  
  as (
    with __dbt__cte__stg_ptrun_pressure as (
-- Staging: pressure detail - PTRUN_PRESSURE_DETAIL


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_PRESSURE_DETAIL
) -- PFR: pressure by fruit size (app filters by unique_run_key in (...))


select *
from __dbt__cte__stg_ptrun_pressure
  );

