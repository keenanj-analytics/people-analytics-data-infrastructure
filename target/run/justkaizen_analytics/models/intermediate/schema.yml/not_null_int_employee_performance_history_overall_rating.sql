
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select overall_rating
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history`
where overall_rating is null



  
  
      
    ) dbt_internal_test