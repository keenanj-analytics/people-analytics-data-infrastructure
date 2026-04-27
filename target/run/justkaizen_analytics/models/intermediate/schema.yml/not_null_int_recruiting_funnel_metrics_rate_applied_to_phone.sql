
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rate_applied_to_phone
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where rate_applied_to_phone is null



  
  
      
    ) dbt_internal_test