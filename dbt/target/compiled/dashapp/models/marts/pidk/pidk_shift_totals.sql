with __dbt__cte__stg_shift_totals as (
-- Staging: shift totals - VW_SHIFT_TOTALS_FAST_03


select
    packdate_run_key,
    date_shift_key,
    shift,
    day_label,
    coalesce(total_bins, 0)::number(12,2) as total_bins,
    coalesce(bins_at_cur_onshift_rate_full_shift, 0)::number(12,2) as bins_at_cur_onshift_rate_full_shift,
    coalesce(bins_target_full_shift, 0)::number(12,2) as bins_target_full_shift,
    bins_per_hour::number(12,2) as bins_per_hour,
    stamper_ppmh::number(12,2) as stamper_ppmh,
    packs_manhour_target_weighted::number(12,2) as packs_manhour_target_weighted,
    bin_hour_target_weighted::number(12,2) as bin_hour_target_weighted,
    stamper_eqs_per_hour::number(12,2) as stamper_eqs_per_hour,
    packs_per_bin::number(12,2) as packs_per_bin,
    is_current_shift,
    current_timestamp() as dbt_loaded_at
from FROSTY.STAGING.VW_SHIFT_TOTALS_FAST_03
) -- PIDK Shift Totals - Production Intra Day KPIs


select
    packdate_run_key as "PACKDATE_RUN_KEY",
    shift as "Shift",
    coalesce(total_bins, 0) as "TotalBins",
    coalesce(bins_at_cur_onshift_rate_full_shift, 0) as "ForcastedBins",
    coalesce(bins_target_full_shift, 0) as "BinsTarget",
    bins_per_hour as "BinPerHour",
    stamper_ppmh as "PPMH",
    packs_manhour_target_weighted as "PPMHTarget",
    bin_hour_target_weighted as "BPHTarget",
    stamper_eqs_per_hour as "EQsPerHour",
    day_label as "DAY_LABEL"
from __dbt__cte__stg_shift_totals