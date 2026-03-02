-- PIDK Run Totals - Production Intra Day KPIs
-- Mart model: replaces the get_run_totals() query in pidk_data.py
-- Created in DBT_DEV only. Source: FROSTY.STAGING (read-only).
-- App queries: SELECT * FROM DBT_DEV.pidk_run_totals WHERE DAY_LABEL = 'TODAY' ORDER BY "Run", "Lot"


with  __dbt__cte__stg_ptrun_report as (
-- Staging: reference to existing dynamic table DQ_PTRUN_N_REPORT_03
-- Ephemeral = no new Snowflake objects. SQL inlined into downstream models.


select * from FROSTY.STAGING.DQ_PTRUN_N_REPORT_03
),  __dbt__cte__stg_run_totals as (
-- Staging: reference to existing dynamic table VW_RUN_TOTALS_FAST_03
-- Ephemeral = no new Snowflake objects. SQL inlined into downstream models.


select * from FROSTY.STAGING.VW_RUN_TOTALS_FAST_03
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