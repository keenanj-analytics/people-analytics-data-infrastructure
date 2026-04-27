
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select top_application_channel_share
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where top_application_channel_share is null



  
  
      
    ) dbt_internal_test