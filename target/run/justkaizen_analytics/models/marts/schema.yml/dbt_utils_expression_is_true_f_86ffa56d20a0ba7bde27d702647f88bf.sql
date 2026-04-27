
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`

where not(time_to_fill_days >= 0)


  
  
      
    ) dbt_internal_test