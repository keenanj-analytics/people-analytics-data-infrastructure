
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hire_year_quarter
from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`
where hire_year_quarter is null



  
  
      
    ) dbt_internal_test