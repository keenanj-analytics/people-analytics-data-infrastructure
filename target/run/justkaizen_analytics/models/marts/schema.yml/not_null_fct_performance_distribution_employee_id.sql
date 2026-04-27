
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select employee_id
from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
where employee_id is null



  
  
      
    ) dbt_internal_test