
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select PACKDATE_RUN_KEY
from FROSTY.DBT_DEV_DBT_DEV.pidk_run_totals
where PACKDATE_RUN_KEY is null



  
  
      
    ) dbt_internal_test