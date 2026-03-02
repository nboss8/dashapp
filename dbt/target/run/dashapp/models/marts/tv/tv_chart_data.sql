
  create or replace   view FROSTY.DBT_DEV_DBT_DEV.tv_chart_data
  
  
  
  
  as (
    with __dbt__cte__stg_shift_10min_kpi as (
-- Staging: 10-min bucket KPIs per run - DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT


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
from FROSTY.STAGING.DT_SHIFT_10MIN_KPI_A_PER_RUN03_DT
) -- TV: chart data aggregated by date_shift_key, bucket_start (BPH and PPMH charts)


select
    date_shift_key,
    bucket_start,
    sum(bins_per_hour) as bins_per_hour,
    avg(bin_hour_target) as bin_hour_target,
    sum(stamper_eqs) / nullif(sum(minutes_worked_alloc) / 60, 0) as est_packs_per_man_hour,
    avg(packs_manhour_target) as packs_manhour_target,
    sum(minutes_worked_alloc) as minutes_elapsed
from __dbt__cte__stg_shift_10min_kpi
group by date_shift_key, bucket_start
order by date_shift_key, bucket_start
  );

