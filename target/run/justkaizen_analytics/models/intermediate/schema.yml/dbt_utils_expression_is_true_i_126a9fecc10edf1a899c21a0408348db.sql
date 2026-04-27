
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`

where not(questions_in_theme between 3 and 5)


  
  
      
    ) dbt_internal_test