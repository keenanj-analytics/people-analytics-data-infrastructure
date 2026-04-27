
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_top_performer
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history`
where is_top_performer is null



  
  
      
    ) dbt_internal_test