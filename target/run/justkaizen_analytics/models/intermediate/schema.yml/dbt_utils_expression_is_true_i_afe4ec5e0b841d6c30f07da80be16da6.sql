
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history`

where not(cycle_sequence >= 1)


  
  
      
    ) dbt_internal_test