
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`

where not(top_source_share between 0 and 1)


  
  
      
    ) dbt_internal_test