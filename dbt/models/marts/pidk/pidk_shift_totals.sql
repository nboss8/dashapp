-- PIDK Shift Totals - Production Intra Day KPIs
{{ config(materialized='view', tags=['pidk', 'marts']) }}

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
from {{ ref('stg_shift_totals') }}
