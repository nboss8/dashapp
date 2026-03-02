-- PIDK Sizer events: join sizer header with ptrun report
{{ config(materialized='view', tags=['pidk', 'marts']) }}

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
from {{ ref('stg_sizer_header') }} h
inner join {{ ref('stg_ptrun_report') }} p on p.run_key = h.shift_key

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
from {{ ref('stg_sizer_header') }} h
inner join {{ ref('stg_ptrun_report') }} p on h.shift_key like p.packdate_run_key || '%'
where not exists (
    select 1 from {{ ref('stg_ptrun_report') }} p2 where p2.run_key = h.shift_key
)
