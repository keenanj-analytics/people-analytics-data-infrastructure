
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select favorable_pct
from `just-kaizen-ai`.`raw_staging`.`stg_engagement`
where favorable_pct is null



  
  
      
    ) dbt_internal_test