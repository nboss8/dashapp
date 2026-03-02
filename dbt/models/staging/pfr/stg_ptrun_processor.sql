-- Staging: processor view - PTRUN_PROCESSOR_VIEW_PBIX
{{ config(materialized='ephemeral', tags=['pfr', 'staging']) }}

select
    *,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_app', 'PTRUN_PROCESSOR_VIEW_PBIX') }}
