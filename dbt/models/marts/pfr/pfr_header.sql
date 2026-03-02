-- PFR: main header rows (app filters by run_date, grower, variety, pool)
{{ config(materialized='view', tags=['pfr', 'marts']) }}

select *
from {{ ref('stg_powerbi_prod_header') }}
