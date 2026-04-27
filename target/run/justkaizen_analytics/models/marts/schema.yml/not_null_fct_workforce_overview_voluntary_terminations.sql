
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select voluntary_terminations
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`
where voluntary_terminations is null



  
  
      
    ) dbt_internal_test