





with validation_errors as (

    select
        employee_id, review_cycle
    from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
    group by employee_id, review_cycle
    having count(*) > 1

)

select *
from validation_errors


