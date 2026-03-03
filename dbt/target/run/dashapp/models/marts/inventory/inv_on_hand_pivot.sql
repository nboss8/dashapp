
  create or replace   view FROSTY.DBT_DEV_DBT_DEV.inv_on_hand_pivot
  
  
  
  
  as (
    -- On-hand inventory pivot/detail. Thin wrapper over DT_INV_ON_HAND_SKU_GRAIN.


select * from FROSTY.DBT_DEV_DBT_DEV.DT_INV_ON_HAND_SKU_GRAIN
  );

