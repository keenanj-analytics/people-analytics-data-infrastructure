
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        employee_id, review_cycle
    from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history`
    group by employee_id, review_cycle
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test