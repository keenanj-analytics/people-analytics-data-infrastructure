
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hiring_manager
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where hiring_manager is null



  
  
      
    ) dbt_internal_test