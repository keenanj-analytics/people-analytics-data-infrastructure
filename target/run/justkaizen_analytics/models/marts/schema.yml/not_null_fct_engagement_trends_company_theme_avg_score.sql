
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select company_theme_avg_score
from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
where company_theme_avg_score is null



  
  
      
    ) dbt_internal_test