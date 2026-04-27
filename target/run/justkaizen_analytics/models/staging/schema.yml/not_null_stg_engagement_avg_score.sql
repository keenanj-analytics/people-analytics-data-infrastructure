
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select avg_score
from `just-kaizen-ai`.`raw_staging`.`stg_engagement`
where avg_score is null



  
  
      
    ) dbt_internal_test