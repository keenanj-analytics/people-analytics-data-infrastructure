
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select has_manager_change_last_12mo
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_tenure`
where has_manager_change_last_12mo is null



  
  
      
    ) dbt_internal_test