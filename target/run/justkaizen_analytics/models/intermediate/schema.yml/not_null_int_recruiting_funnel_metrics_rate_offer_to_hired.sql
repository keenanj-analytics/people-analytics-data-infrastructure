
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rate_offer_to_hired
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where rate_offer_to_hired is null



  
  
      
    ) dbt_internal_test