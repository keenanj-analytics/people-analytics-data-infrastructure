
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rate_phone_to_onsite
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where rate_phone_to_onsite is null



  
  
      
    ) dbt_internal_test