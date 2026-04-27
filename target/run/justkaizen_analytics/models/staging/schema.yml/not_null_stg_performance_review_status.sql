
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select review_status
from `just-kaizen-ai`.`raw_staging`.`stg_performance`
where review_status is null



  
  
      
    ) dbt_internal_test