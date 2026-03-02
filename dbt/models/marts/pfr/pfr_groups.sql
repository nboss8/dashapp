-- PFR: groups for dropdown (group by run_date, grower, variety, pool)
{{ config(materialized='view', tags=['pfr', 'marts']) }}

select
    run_date,
    grower,
    variety_user_cd,
    pool,
    count(*) as runs,
    sum(bins_submitted) as bins,
    sum(actual_net) as net
from {{ ref('stg_powerbi_prod_header') }}
where coalesce(bins_submitted, 0) > 0
group by run_date, grower, variety_user_cd, pool
order by run_date, grower, variety_user_cd, pool
