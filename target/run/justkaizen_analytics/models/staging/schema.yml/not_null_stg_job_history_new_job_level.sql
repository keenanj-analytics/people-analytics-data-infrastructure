
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select new_job_level
from `just-kaizen-ai`.`raw_staging`.`stg_job_history`
where new_job_level is null



  
  
      
    ) dbt_internal_test