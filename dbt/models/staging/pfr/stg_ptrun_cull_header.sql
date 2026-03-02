-- Staging: cull header - PTRUN_CULL_HEADER
{{ config(materialized='ephemeral', tags=['pfr', 'staging']) }}

select
    *,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_app', 'PTRUN_CULL_HEADER') }}
