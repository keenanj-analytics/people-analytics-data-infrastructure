
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tenure_at_termination_years
from `just-kaizen-ai`.`raw_marts`.`fct_attrition_drivers`
where tenure_at_termination_years is null



  
  
      
    ) dbt_internal_test