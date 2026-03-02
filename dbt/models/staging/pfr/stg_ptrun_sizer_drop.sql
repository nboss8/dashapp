-- Staging: sizer drop snapshot - PTRUN_SIZER_DROP_SNAPSHOT
{{ config(materialized='ephemeral', tags=['pfr', 'staging']) }}

select
    *,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_app', 'PTRUN_SIZER_DROP_SNAPSHOT') }}
