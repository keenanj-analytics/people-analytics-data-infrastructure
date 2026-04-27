
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from (select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history` where self_rating_numeric is not null) dbt_subquery

where not(self_rating_numeric between 1 and 5)


  
  
      
    ) dbt_internal_test