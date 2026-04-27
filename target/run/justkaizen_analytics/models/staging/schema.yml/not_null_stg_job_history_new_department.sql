
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select new_department
from `just-kaizen-ai`.`raw_staging`.`stg_job_history`
where new_department is null



  
  
      
    ) dbt_internal_test