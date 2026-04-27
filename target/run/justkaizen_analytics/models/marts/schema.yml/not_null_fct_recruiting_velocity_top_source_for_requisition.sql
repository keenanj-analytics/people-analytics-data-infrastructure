
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select top_source_for_requisition
from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`
where top_source_for_requisition is null



  
  
      
    ) dbt_internal_test