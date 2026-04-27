
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_applications
from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`
where total_applications is null



  
  
      
    ) dbt_internal_test