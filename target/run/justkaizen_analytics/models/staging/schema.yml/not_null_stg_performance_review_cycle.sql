
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select review_cycle
from `just-kaizen-ai`.`raw_staging`.`stg_performance`
where review_cycle is null



  
  
      
    ) dbt_internal_test