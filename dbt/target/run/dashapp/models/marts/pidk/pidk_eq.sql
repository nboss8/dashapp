
  create or replace   view FROSTY.DBT_DEV_DBT_DEV.pidk_eq
  
  
  
  
  as (
    with __dbt__cte__stg_eq_cartons as (
-- Staging: EQ cartons with run keys - DQ_EQ_WITH_KEYS03


select
    run_key,
    pack_abbr,
    grade_abbr,
    coalesce(cartons, 0)::number(12,2) as cartons,
    coalesce(eq_on_hand, cartons, 0)::number(12,2) as eq_on_hand,
    status,
    pallet_ticket,
    variety_abbr,
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.DQ_EQ_WITH_KEYS03
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
),  __dbt__cte__stg_pack_classification as (
-- Staging: pack abbr to classification - PACK_CLASSIFICATION


select
    trim(pack_abbr) as pack_abbr,
    coalesce(nullif(trim(classification), ''), 'Unclassified') as classification,
    pack_type,
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.PACK_CLASSIFICATION
) -- PIDK EQ: matrix + package types (join eq + ptrun + pack_classification)


select
    trim(e.pack_abbr) as pack_abbr,
    trim(e.grade_abbr) as grade_abbr,
    coalesce(e.cartons, 0) as cartons,
    coalesce(e.eq_on_hand, e.cartons, 0) as eq_val,
    coalesce(nullif(trim(pc.classification), ''), 'Unclassified') as classification,
    p.packdate_run_key,
    p.run_key,
    p.day_label
from __dbt__cte__stg_eq_cartons e
inner join __dbt__cte__stg_ptrun_report p on p.run_key = e.run_key
left join __dbt__cte__stg_pack_classification pc
    on upper(trim(e.pack_abbr)) = upper(trim(pc.pack_abbr))
  );

