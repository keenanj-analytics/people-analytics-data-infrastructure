
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select terminated_within_6mo_of_manager_change
from `just-kaizen-ai`.`raw_marts`.`fct_attrition_drivers`
where terminated_within_6mo_of_manager_change is null



  
  
      
    ) dbt_internal_test