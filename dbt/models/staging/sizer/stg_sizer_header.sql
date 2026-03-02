-- Staging: sizer batch header/events - DQ_APPLE_SIZER_HEADER_VIEW_03
{{ config(materialized='ephemeral', tags=['sizer', 'staging']) }}

select
    "BatchID" as batch_id,
    "EventId" as event_id,
    "SHIFT_KEY" as shift_key,
    "GrowerCode" as grower_code,
    "VarietyName" as variety_name,
    "StartTime" as start_time,
    "SHIFT_CODE" as shift_code,
    "EndTime",
    "Bins",
    "SHIFT_NAME",
    "SHIFT_DETAILS",
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_staging', 'DQ_APPLE_SIZER_HEADER_VIEW_03') }}
