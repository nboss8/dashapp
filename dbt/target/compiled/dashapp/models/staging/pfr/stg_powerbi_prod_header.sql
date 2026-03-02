-- Staging: production header materialized - POWERBI_PRODUCTION_HEADER_MAT


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.POWERBI_PRODUCTION_HEADER_MAT