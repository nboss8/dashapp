-- TV: chart data aggregated by date_shift_key, bucket_start (BPH and PPMH charts)
{{ config(materialized='view', tags=['tv', 'marts']) }}

select
    date_shift_key,
    bucket_start,
    sum(bins_per_hour) as bins_per_hour,
    avg(bin_hour_target) as bin_hour_target,
    sum(stamper_eqs) / nullif(sum(minutes_worked_alloc) / 60, 0) as est_packs_per_man_hour,
    avg(packs_manhour_target) as packs_manhour_target,
    sum(minutes_worked_alloc) as minutes_elapsed
from {{ ref('stg_shift_10min_kpi') }}
group by date_shift_key, bucket_start
order by date_shift_key, bucket_start
