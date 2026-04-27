
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select time_in_current_role_months
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where time_in_current_role_months is null



  
  
      
    ) dbt_internal_test