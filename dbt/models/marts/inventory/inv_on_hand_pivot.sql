-- On-hand inventory pivot/detail. Thin wrapper over DT_INV_ON_HAND_SKU_GRAIN.
{{ config(materialized='view', tags=['inventory', 'marts']) }}

select * from {{ source('frosty_dbt_dev', 'DT_INV_ON_HAND_SKU_GRAIN') }}
