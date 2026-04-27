
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select first_name
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where first_name is null



  
  
      
    ) dbt_internal_test