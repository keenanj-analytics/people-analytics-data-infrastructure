
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select question_text
from `just-kaizen-ai`.`raw_staging`.`stg_engagement`
where question_text is null



  
  
      
    ) dbt_internal_test