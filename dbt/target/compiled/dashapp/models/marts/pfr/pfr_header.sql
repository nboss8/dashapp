with __dbt__cte__stg_powerbi_prod_header as (
-- Staging: production header materialized - POWERBI_PRODUCTION_HEADER_MAT


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT
) -- PFR: main header rows (app filters by run_date, grower, variety, pool)


select *
from __dbt__cte__stg_powerbi_prod_header