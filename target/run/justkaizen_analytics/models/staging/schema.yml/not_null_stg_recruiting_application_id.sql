
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select application_id
from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
where application_id is null



  
  
      
    ) dbt_internal_test