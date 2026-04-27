
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select survey_cycle
from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
where survey_cycle is null



  
  
      
    ) dbt_internal_test