
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select manager_rating
from `just-kaizen-ai`.`raw_staging`.`stg_performance`
where manager_rating is null



  
  
      
    ) dbt_internal_test