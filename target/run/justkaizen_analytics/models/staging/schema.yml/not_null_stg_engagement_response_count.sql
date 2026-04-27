
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select response_count
from `just-kaizen-ai`.`raw_staging`.`stg_engagement`
where response_count is null



  
  
      
    ) dbt_internal_test