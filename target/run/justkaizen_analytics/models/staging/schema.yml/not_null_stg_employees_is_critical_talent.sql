
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_critical_talent
from `just-kaizen-ai`.`raw_staging`.`stg_employees`
where is_critical_talent is null



  
  
      
    ) dbt_internal_test