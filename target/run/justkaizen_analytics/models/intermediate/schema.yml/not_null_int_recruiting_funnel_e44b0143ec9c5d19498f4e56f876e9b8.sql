
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select top_application_channel_count
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where top_application_channel_count is null



  
  
      
    ) dbt_internal_test