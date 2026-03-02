-- Staging: cull header - PTRUN_CULL_HEADER


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_CULL_HEADER