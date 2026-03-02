with __dbt__cte__stg_shift_totals as (
-- Staging: reference to existing dynamic table VW_SHIFT_TOTALS_FAST_03
-- Ephemeral = no new Snowflake objects. SQL inlined into downstream models.


select * from FROSTY.STAGING.VW_SHIFT_TOTALS_FAST_03
) -- PIDK Shift Totals - Production Intra Day KPIs
-- Mart model: replaces the get_shift_totals() query in pidk_data.py
-- Created in DBT_DEV only. Source: FROSTY.STAGING (read-only).
-- App queries: SELECT * FROM DBT_DEV.pidk_shift_totals WHERE day_label = 'TODAY' ORDER BY shift


select
    v.packdate_run_key as "PACKDATE_RUN_KEY",
    v.shift as "Shift",
    coalesce(v.total_bins, 0) as "TotalBins",
    coalesce(v.bins_at_cur_onshift_rate_full_shift, 0) as "ForcastedBins",
    coalesce(v.bins_target_full_shift, 0) as "BinsTarget",
    v.bins_per_hour as "BinPerHour",
    v.stamper_ppmh as "PPMH",
    v.packs_manhour_target_weighted as "PPMHTarget",
    v.bin_hour_target_weighted as "BPHTarget",
    v.stamper_eqs_per_hour as "EQsPerHour",
    v.day_label as "DAY_LABEL"
from __dbt__cte__stg_shift_totals v