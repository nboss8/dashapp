-- On-hand inventory pivot/detail. Thin wrapper over DT_INV_ON_HAND_SKU_GRAIN.
{{ config(materialized='view', tags=['inventory', 'marts']) }}

select
  GROUP_CATEGORY,
  FINAL_STAGE_STATUS,
  VARIETY_ABBR,
  SKU,
  WEEK_BUCKET,
  WEEK_BUCKET_NUM,
  PACK_ABBR,
  GRADE_ABBR,
  SIZE_ABBR,
  POOL,
  "Process Code",
  CARTONS,
  EQ_ON_HAND,
  CARTONS_AVAIL
from {{ source('frosty_dbt_dev', 'DT_INV_ON_HAND_SKU_GRAIN') }}
