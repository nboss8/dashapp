-- Staging: sizer packed - PTRUN_SIZER_PACKED
{{ config(materialized='ephemeral', tags=['pfr', 'staging']) }}

select
    *,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_app', 'PTRUN_SIZER_PACKED') }}
