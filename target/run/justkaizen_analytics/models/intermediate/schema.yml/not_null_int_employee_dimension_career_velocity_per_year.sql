
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select career_velocity_per_year
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where career_velocity_per_year is null



  
  
      
    ) dbt_internal_test