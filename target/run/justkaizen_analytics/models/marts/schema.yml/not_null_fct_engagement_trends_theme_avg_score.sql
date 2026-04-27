
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select theme_avg_score
from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
where theme_avg_score is null



  
  
      
    ) dbt_internal_test