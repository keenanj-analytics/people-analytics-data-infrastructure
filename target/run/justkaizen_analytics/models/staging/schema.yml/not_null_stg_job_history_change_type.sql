
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select change_type
from `just-kaizen-ai`.`raw_staging`.`stg_job_history`
where change_type is null



  
  
      
    ) dbt_internal_test