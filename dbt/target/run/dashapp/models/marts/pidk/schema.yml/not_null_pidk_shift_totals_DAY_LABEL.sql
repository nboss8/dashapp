
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select DAY_LABEL
from FROSTY.DBT_DEV_DBT_DEV.pidk_shift_totals
where DAY_LABEL is null



  
  
      
    ) dbt_internal_test