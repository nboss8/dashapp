-- Staging: lot dumper time, IS_CURRENT_LOT - VW_LOT_DUMPER_TIME_03


select
    date_shift_key,
    run_key,
    is_current_lot,
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.VW_LOT_DUMPER_TIME_03