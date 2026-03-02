-- Staging: sizer drop snapshot - PTRUN_SIZER_DROP_SNAPSHOT


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_SIZER_DROP_SNAPSHOT