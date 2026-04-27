
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select count_withdrawn
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where count_withdrawn is null



  
  
      
    ) dbt_internal_test