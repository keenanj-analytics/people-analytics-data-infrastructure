
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select employee_id
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_compensation_current`
where employee_id is null



  
  
      
    ) dbt_internal_test