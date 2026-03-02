-- Staging: production header materialized - POWERBI_PRODUCTION_HEADER_MAT
{{ config(materialized='ephemeral', tags=['pfr', 'staging']) }}

select
    *,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_app', 'POWERBI_PRODUCTION_HEADER_MAT') }}
