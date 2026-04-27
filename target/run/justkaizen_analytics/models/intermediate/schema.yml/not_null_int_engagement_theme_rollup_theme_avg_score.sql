
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select theme_avg_score
from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`
where theme_avg_score is null



  
  
      
    ) dbt_internal_test