
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reached_offer
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where reached_offer is null



  
  
      
    ) dbt_internal_test