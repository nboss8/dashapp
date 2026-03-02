
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    RUN_KEY as unique_field,
    count(*) as n_records

from FROSTY.DBT_DEV_DBT_DEV.pidk_run_totals
where RUN_KEY is not null
group by RUN_KEY
having count(*) > 1



  
  
      
    ) dbt_internal_test