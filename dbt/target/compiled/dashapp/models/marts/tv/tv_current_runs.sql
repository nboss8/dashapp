with __dbt__cte__stg_run_totals as (
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
),  __dbt__cte__stg_lot_dumper_time as (
-- Staging: lot dumper time, IS_CURRENT_LOT - VW_LOT_DUMPER_TIME_03


select
    date_shift_key,
    run_key,
    is_current_lot,
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.VW_LOT_DUMPER_TIME_03
) -- TV: current runs (run_totals joined with lot_dumper where IS_CURRENT_LOT=1)
-- App filters by date_shift_key


select
    v.date_shift_key,
    v.grower_number,
    v.variety_abbr,
    v.shift,
    coalesce(v.bins_on_shift, 0) + coalesce(v.bins_pre_shift, 0) as bins,
    v.bins_per_hour,
    v.stamper_ppmh,
    v.bin_hour_target,
    v.packs_manhour_target,
    v.bins_target_color,
    v.packs_target_color
from __dbt__cte__stg_run_totals v
inner join __dbt__cte__stg_lot_dumper_time l
    on l.date_shift_key = v.date_shift_key
    and l.run_key = v.run_key
where l.is_current_lot = 1