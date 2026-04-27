
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select event_sequence
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_event_sequence`
where event_sequence is null



  
  
      
    ) dbt_internal_test