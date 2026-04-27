
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_attrition_drivers`

where not(tenure_at_termination_months >= 0)


  
  
      
    ) dbt_internal_test