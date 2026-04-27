
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_latest_event
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_event_sequence`
where is_latest_event is null



  
  
      
    ) dbt_internal_test