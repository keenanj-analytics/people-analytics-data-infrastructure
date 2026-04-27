
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hired_phone_screen_date
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where hired_phone_screen_date is null



  
  
      
    ) dbt_internal_test