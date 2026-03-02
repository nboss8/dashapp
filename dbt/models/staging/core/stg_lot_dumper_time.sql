-- Staging: lot dumper time, IS_CURRENT_LOT - VW_LOT_DUMPER_TIME_03
{{ config(materialized='ephemeral', tags=['core', 'staging']) }}

select
    date_shift_key,
    run_key,
    is_current_lot,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_staging', 'VW_LOT_DUMPER_TIME_03') }}
