
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cycle_sequence
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history`
where cycle_sequence is null



  
  
      
    ) dbt_internal_test