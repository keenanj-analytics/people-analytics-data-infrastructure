
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select time_to_offer_days
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where time_to_offer_days is null



  
  
      
    ) dbt_internal_test