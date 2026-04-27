
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select requisition_id
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
where requisition_id is null



  
  
      
    ) dbt_internal_test