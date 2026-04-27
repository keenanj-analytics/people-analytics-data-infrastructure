
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select first_application_date
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where first_application_date is null



  
  
      
    ) dbt_internal_test