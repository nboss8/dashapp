-- Staging: PTRUN report (day labels, run keys) - DQ_PTRUN_N_REPORT_03


select
    run_key,
    packdate_run_key,
    runs,
    shift,
    grower_number,
    day_label,
    date_d,
    variety_list,
    variety_abbr_list,
    kpi_variety_name_list,
    coalesce(bin_hour_target, 0)::number(12,2) as bin_hour_target,
    coalesce(packs_manhour_target, 0)::number(12,2) as packs_manhour_target,
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.DQ_PTRUN_N_REPORT_03