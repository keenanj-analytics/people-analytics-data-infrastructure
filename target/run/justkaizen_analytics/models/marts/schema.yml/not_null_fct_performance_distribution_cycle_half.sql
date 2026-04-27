
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cycle_half
from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
where cycle_half is null



  
  
      
    ) dbt_internal_test