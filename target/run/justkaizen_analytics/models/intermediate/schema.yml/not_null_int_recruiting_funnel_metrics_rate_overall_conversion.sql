
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rate_overall_conversion
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where rate_overall_conversion is null



  
  
      
    ) dbt_internal_test