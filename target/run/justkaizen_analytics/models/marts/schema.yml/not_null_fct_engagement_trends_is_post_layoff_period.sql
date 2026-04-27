
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_post_layoff_period
from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
where is_post_layoff_period is null



  
  
      
    ) dbt_internal_test