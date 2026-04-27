
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select candidate_name
from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
where candidate_name is null



  
  
      
    ) dbt_internal_test