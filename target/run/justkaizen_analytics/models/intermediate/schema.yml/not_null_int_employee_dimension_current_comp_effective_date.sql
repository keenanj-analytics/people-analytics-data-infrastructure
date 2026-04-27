
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select current_comp_effective_date
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where current_comp_effective_date is null



  
  
      
    ) dbt_internal_test