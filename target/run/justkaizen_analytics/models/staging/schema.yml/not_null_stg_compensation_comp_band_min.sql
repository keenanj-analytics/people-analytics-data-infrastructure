
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select comp_band_min
from `just-kaizen-ai`.`raw_staging`.`stg_compensation`
where comp_band_min is null



  
  
      
    ) dbt_internal_test