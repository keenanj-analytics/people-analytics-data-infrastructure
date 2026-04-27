
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select end_headcount
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`
where end_headcount is null



  
  
      
    ) dbt_internal_test