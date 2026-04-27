
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select questions_in_theme
from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`
where questions_in_theme is null



  
  
      
    ) dbt_internal_test