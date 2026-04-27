
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select employment_status
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where employment_status is null



  
  
      
    ) dbt_internal_test