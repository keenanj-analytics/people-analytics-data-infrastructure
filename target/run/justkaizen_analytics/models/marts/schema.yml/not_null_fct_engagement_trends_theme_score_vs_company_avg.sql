
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select theme_score_vs_company_avg
from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
where theme_score_vs_company_avg is null



  
  
      
    ) dbt_internal_test