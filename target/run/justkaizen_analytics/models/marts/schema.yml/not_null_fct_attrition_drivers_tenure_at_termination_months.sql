
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select tenure_at_termination_months
from `just-kaizen-ai`.`raw_marts`.`fct_attrition_drivers`
where tenure_at_termination_months is null



  
  
      
    ) dbt_internal_test