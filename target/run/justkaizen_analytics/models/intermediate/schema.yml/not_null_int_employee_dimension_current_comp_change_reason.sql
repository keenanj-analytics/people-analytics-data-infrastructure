
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select current_comp_change_reason
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where current_comp_change_reason is null



  
  
      
    ) dbt_internal_test