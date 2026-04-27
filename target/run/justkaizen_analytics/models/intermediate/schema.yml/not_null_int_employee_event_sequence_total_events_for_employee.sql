
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_events_for_employee
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_event_sequence`
where total_events_for_employee is null



  
  
      
    ) dbt_internal_test