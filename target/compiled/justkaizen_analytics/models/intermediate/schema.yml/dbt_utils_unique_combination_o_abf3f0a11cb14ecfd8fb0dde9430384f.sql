





with validation_errors as (

    select
        employee_id, review_cycle
    from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history`
    group by employee_id, review_cycle
    having count(*) > 1

)

select *
from validation_errors


