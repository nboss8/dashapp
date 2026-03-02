
  create or replace   view FROSTY.DBT_DEV_DBT_DEV.pfr_groups
  
  
  
  
  as (
    with __dbt__cte__stg_powerbi_prod_header as (
-- Staging: production header materialized - POWERBI_PRODUCTION_HEADER_MAT


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT
) -- PFR: groups for dropdown (group by run_date, grower, variety, pool)


select
    run_date,
    grower,
    variety_user_cd,
    pool,
    count(*) as runs,
    sum(bins_submitted) as bins,
    sum(actual_net) as net
from __dbt__cte__stg_powerbi_prod_header
where coalesce(bins_submitted, 0) > 0
group by run_date, grower, variety_user_cd, pool
order by run_date, grower, variety_user_cd, pool
  );

