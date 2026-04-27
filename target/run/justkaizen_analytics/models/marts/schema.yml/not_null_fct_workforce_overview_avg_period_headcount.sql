
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_period_headcount
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`
where avg_period_headcount is null



  
  
      
    ) dbt_internal_test