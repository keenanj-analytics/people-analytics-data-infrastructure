
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_tenure`

where not(career_velocity_per_year >= 0)


  
  
      
    ) dbt_internal_test