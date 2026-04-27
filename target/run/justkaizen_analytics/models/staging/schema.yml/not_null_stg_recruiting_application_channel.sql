
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select application_channel
from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
where application_channel is null



  
  
      
    ) dbt_internal_test