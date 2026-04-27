
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from (select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history` where overall_rating_delta is not null) dbt_subquery

where not(overall_rating_delta between -4 and 4)


  
  
      
    ) dbt_internal_test