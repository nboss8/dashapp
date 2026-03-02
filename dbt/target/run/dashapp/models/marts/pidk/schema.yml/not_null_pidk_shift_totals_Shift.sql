
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select Shift
from FROSTY.DBT_DEV_DBT_DEV.pidk_shift_totals
where Shift is null



  
  
      
    ) dbt_internal_test