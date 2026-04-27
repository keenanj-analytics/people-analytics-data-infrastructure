
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`

where not(overall_rating_numeric between 1 and 5)


  
  
      
    ) dbt_internal_test