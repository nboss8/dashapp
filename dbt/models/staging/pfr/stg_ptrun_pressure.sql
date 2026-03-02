-- Staging: pressure detail - PTRUN_PRESSURE_DETAIL
{{ config(materialized='ephemeral', tags=['pfr', 'staging']) }}

select
    *,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_app', 'PTRUN_PRESSURE_DETAIL') }}
