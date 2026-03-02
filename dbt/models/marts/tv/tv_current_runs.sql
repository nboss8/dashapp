-- TV: current runs (run_totals joined with lot_dumper where IS_CURRENT_LOT=1)
-- App filters by date_shift_key
{{ config(materialized='view', tags=['tv', 'marts']) }}

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
from {{ ref('stg_run_totals') }} v
inner join {{ ref('stg_lot_dumper_time') }} l
    on l.date_shift_key = v.date_shift_key
    and l.run_key = v.run_key
where l.is_current_lot = 1
