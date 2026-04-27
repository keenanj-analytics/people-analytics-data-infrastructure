
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select count_hired
from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`
where count_hired is null



  
  
      
    ) dbt_internal_test