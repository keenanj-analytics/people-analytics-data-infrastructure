
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_applications
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where total_applications is null



  
  
      
    ) dbt_internal_test