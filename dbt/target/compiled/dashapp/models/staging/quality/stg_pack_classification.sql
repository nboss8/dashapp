-- Staging: pack abbr to classification - PACK_CLASSIFICATION


select
    trim(pack_abbr) as pack_abbr,
    coalesce(nullif(trim(classification), ''), 'Unclassified') as classification,
    pack_type,
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.PACK_CLASSIFICATION