
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cycle_year
from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
where cycle_year is null



  
  
      
    ) dbt_internal_test