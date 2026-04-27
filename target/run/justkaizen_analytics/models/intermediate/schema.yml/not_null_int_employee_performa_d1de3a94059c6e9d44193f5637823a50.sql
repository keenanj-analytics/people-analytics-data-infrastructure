
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select manager_rating_numeric
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history`
where manager_rating_numeric is null



  
  
      
    ) dbt_internal_test