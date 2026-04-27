
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`

where not(theme_avg_score between 1 and 5)


  
  
      
    ) dbt_internal_test