
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select change_reason
from `just-kaizen-ai`.`raw_staging`.`stg_compensation`
where change_reason is null



  
  
      
    ) dbt_internal_test