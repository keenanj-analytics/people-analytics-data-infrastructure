
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select reached_phone_screen
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where reached_phone_screen is null



  
  
      
    ) dbt_internal_test