-- PIDK Sizer drops: drop summary per event
{{ config(materialized='view', tags=['pidk', 'marts']) }}

select *
from {{ ref('stg_sizer_dropsummary') }}
