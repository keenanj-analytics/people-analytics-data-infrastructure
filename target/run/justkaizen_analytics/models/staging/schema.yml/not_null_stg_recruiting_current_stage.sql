
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select current_stage
from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
where current_stage is null



  
  
      
    ) dbt_internal_test