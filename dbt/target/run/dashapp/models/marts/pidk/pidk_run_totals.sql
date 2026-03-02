
  create or replace   view FROSTY.DBT_DEV_DBT_DEV.pidk_run_totals
  
  
  
  
  as (
    -- PIDK Run Totals - Production Intra Day KPIs


with  __dbt__cte__stg_ptrun_report as (
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
),  __dbt__cte__stg_run_totals as (
-- Staging: run totals (bins, BPH, PPMH) - VW_RUN_TOTALS_FAST_03


select
    packdate_run_key,
    run_key,
    date_shift_key,
    grower_number,
    variety_abbr,
    shift,
    coalesce(bins_pre_shift, 0)::number(12,2) as bins_pre_shift,
    coalesce(bins_on_shift, 0)::number(12,2) as bins_on_shift,
    bins_per_hour::number(12,2) as bins_per_hour,
    stamper_ppmh::number(12,2) as stamper_ppmh,
    coalesce(bin_hour_target, 0)::number(12,2) as bin_hour_target,
    coalesce(packs_manhour_target, 0)::number(12,2) as packs_manhour_target,
    bins_target_color,
    packs_target_color,
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.VW_RUN_TOTALS_FAST_03
), ptrun as (
    select * from __dbt__cte__stg_ptrun_report
),
run_totals as (
    select * from __dbt__cte__stg_run_totals
)
select
    p.run_key as "RUN_KEY",
    p.packdate_run_key as "PACKDATE_RUN_KEY",
    p.runs as "Run",
    v.variety_abbr as "Variety",
    p.shift as "Shift",
    p.grower_number as "Lot",
    coalesce(v.bins_pre_shift, 0) as "BinsPreShift",
    coalesce(v.bins_on_shift, 0) as "BinsOnShift",
    v.bins_per_hour as "BinsPerHour",
    v.stamper_ppmh as "StamperPPMH",
    coalesce(p.bin_hour_target, v.bin_hour_target) as "BinPerHourTarget",
    coalesce(p.packs_manhour_target, v.packs_manhour_target) as "PacksPerHourManHour",
    v.bins_target_color as "BINS_TARGET_COLOR",
    v.packs_target_color as "PACKS_TARGET_COLOR",
    p.day_label as "DAY_LABEL"
from ptrun p
inner join run_totals v
    on v.packdate_run_key = p.packdate_run_key
    and v.grower_number = p.grower_number
  );

