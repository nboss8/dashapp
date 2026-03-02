-- TV: shift totals for KPI cards
{{ config(materialized='view', tags=['tv', 'marts']) }}

select *
from {{ ref('stg_shift_totals') }}
