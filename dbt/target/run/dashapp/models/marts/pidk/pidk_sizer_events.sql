
  create or replace   view FROSTY.DBT_DEV_DBT_DEV.pidk_sizer_events
  
  
  
  
  as (
    with __dbt__cte__stg_sizer_header as (
-- Staging: sizer batch header/events - DQ_APPLE_SIZER_HEADER_VIEW_03


select
    "BatchID" as batch_id,
    "EventId" as event_id,
    "SHIFT_KEY" as shift_key,
    "GrowerCode" as grower_code,
    "VarietyName" as variety_name,
    "StartTime" as start_time,
    "SHIFT_CODE" as shift_code,
    "EndTime",
    "Bins",
    "SHIFT_NAME",
    "SHIFT_DETAILS",
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.DQ_APPLE_SIZER_HEADER_VIEW_03
),  __dbt__cte__stg_ptrun_report as (
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
) -- PIDK Sizer events: join sizer header with ptrun report


select
    h.batch_id as "BatchID",
    h.event_id as "EventId",
    h.shift_key as "SHIFT_KEY",
    h.grower_code as "GrowerCode",
    h.variety_name as "VarietyName",
    h.start_time as "StartTime",
    h.shift_code as "SHIFT_CODE",
    p.packdate_run_key as "PACKDATE_RUN_KEY",
    p.run_key as "RUN_KEY",
    p.day_label as "DAY_LABEL"
from __dbt__cte__stg_sizer_header h
inner join __dbt__cte__stg_ptrun_report p on p.run_key = h.shift_key

union

select
    h.batch_id as "BatchID",
    h.event_id as "EventId",
    h.shift_key as "SHIFT_KEY",
    h.grower_code as "GrowerCode",
    h.variety_name as "VarietyName",
    h.start_time as "StartTime",
    h.shift_code as "SHIFT_CODE",
    p.packdate_run_key as "PACKDATE_RUN_KEY",
    p.run_key as "RUN_KEY",
    p.day_label as "DAY_LABEL"
from __dbt__cte__stg_sizer_header h
inner join __dbt__cte__stg_ptrun_report p on h.shift_key like p.packdate_run_key || '%'
where not exists (
    select 1 from __dbt__cte__stg_ptrun_report p2 where p2.run_key = h.shift_key
)
  );

