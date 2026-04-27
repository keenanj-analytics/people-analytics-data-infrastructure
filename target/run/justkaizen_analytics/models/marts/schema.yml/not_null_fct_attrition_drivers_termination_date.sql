
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select termination_date
from `just-kaizen-ai`.`raw_marts`.`fct_attrition_drivers`
where termination_date is null



  
  
      
    ) dbt_internal_test