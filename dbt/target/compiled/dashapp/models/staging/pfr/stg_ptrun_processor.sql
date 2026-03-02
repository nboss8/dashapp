-- Staging: processor view - PTRUN_PROCESSOR_VIEW_PBIX


select
    *,
    current_timestamp() as dbt_loaded_at
from FROSTY.APP.PTRUN_PROCESSOR_VIEW_PBIX