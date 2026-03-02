-- Staging: sizer packed - PTRUN_SIZER_PACKED


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_SIZER_PACKED