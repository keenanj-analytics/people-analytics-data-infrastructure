
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`

where not(reached_offer >= 1)


  
  
      
    ) dbt_internal_test