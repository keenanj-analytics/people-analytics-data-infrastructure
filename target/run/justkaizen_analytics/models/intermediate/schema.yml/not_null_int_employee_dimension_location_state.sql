
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_state
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where location_state is null



  
  
      
    ) dbt_internal_test