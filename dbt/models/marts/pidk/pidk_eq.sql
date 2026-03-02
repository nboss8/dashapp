-- PIDK EQ: matrix + package types (join eq + ptrun + pack_classification)
{{ config(materialized='view', tags=['pidk', 'marts']) }}

select
    trim(e.pack_abbr) as pack_abbr,
    trim(e.grade_abbr) as grade_abbr,
    coalesce(e.cartons, 0) as cartons,
    coalesce(e.eq_on_hand, e.cartons, 0) as eq_val,
    coalesce(nullif(trim(pc.classification), ''), 'Unclassified') as classification,
    p.packdate_run_key,
    p.run_key,
    p.day_label
from {{ ref('stg_eq_cartons') }} e
inner join {{ ref('stg_ptrun_report') }} p on p.run_key = e.run_key
left join {{ ref('stg_pack_classification') }} pc
    on upper(trim(e.pack_abbr)) = upper(trim(pc.pack_abbr))
