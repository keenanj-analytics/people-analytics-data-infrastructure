
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select net_change
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`
where net_change is null



  
  
      
    ) dbt_internal_test