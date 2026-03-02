-- Staging: run totals (bins, BPH, PPMH) - VW_RUN_TOTALS_FAST_03
{{ config(materialized='ephemeral', tags=['core', 'staging']) }}

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
from {{ source('frosty_staging', 'VW_RUN_TOTALS_FAST_03') }}
