-- Staging: 10-min bucket KPIs per run - DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT
{{ config(materialized='ephemeral', tags=['core', 'staging']) }}

select
    date_shift_key,
    run_key,
    shift,
    bucket_start,
    coalesce(minutes_worked_alloc, 0)::number(10,2) as minutes_worked_alloc,
    coalesce(bins_per_hour, 0)::number(12,2) as bins_per_hour,
    bin_hour_target::number(12,2) as bin_hour_target,
    stamper_eqs,
    packs_manhour_target,
    employee_count_alloc,
    minutes_elapsed,
    bins_dumped,
    sizer_packout_pct,
    sizer_packs_est_21,
    est_packs_per_man_hour,
    stamper_cartons,
    stamper_weight,
    stamper_eqs_per_hour,
    current_timestamp() as dbt_loaded_at
from {{ source('frosty_staging', 'DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT') }}
