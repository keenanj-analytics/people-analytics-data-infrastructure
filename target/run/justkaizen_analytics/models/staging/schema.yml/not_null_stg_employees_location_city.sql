
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select location_city
from `just-kaizen-ai`.`raw_staging`.`stg_employees`
where location_city is null



  
  
      
    ) dbt_internal_test