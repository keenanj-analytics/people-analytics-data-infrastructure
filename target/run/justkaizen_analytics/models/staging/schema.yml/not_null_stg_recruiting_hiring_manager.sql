
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hiring_manager
from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
where hiring_manager is null



  
  
      
    ) dbt_internal_test