
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tenure_months
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_tenure`
where tenure_months is null



  
  
      
    ) dbt_internal_test