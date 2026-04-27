
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select enps_score
from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`
where enps_score is null



  
  
      
    ) dbt_internal_test