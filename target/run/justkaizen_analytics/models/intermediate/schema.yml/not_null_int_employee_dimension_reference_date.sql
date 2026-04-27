
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reference_date
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where reference_date is null



  
  
      
    ) dbt_internal_test