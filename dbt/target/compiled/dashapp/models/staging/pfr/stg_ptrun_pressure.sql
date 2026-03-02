-- Staging: pressure detail - PTRUN_PRESSURE_DETAIL


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_PRESSURE_DETAIL