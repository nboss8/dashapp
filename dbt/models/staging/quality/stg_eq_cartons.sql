-- Staging: EQ cartons with run keys - DQ_EQ_WITH_KEYS03
{{ config(materialized='ephemeral', tags=['quality', 'staging']) }}

select
    run_key,
    pack_abbr,
    grade_abbr,
    coalesce(cartons, 0)::number(12,2) as cartons,
    coalesce(eq_on_hand, cartons, 0)::number(12,2) as eq_on_hand,
    status,
    pallet_ticket,
    variety_abbr,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_staging', 'DQ_EQ_WITH_KEYS03') }}
