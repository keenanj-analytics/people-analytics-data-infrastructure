
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hires_in_period
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`
where hires_in_period is null



  
  
      
    ) dbt_internal_test