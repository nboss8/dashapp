-- Staging: pack abbr to classification - PACK_CLASSIFICATION
{{ config(materialized='ephemeral', tags=['quality', 'staging']) }}

select
    trim(pack_abbr) as pack_abbr,
    coalesce(nullif(trim(classification), ''), 'Unclassified') as classification,
    pack_type,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_staging', 'PACK_CLASSIFICATION') }}
