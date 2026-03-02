-- Staging: cull defect - PTRUN_CULL_DEFECT


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_CULL_DEFECT