
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select effective_date
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_event_sequence`
where effective_date is null



  
  
      
    ) dbt_internal_test