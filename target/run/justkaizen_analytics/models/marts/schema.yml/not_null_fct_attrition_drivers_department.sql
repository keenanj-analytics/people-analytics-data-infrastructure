
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select department
from `just-kaizen-ai`.`raw_marts`.`fct_attrition_drivers`
where department is null



  
  
      
    ) dbt_internal_test