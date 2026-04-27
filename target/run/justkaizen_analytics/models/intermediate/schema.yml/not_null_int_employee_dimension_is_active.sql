
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_active
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where is_active is null



  
  
      
    ) dbt_internal_test