





with validation_errors as (

    select
        employee_id, event_sequence
    from `just-kaizen-ai`.`raw_intermediate`.`int_employee_event_sequence`
    group by employee_id, event_sequence
    having count(*) > 1

)

select *
from validation_errors


