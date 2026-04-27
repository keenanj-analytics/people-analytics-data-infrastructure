
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select survey_cycle
from `just-kaizen-ai`.`raw_staging`.`stg_engagement`
where survey_cycle is null



  
  
      
    ) dbt_internal_test