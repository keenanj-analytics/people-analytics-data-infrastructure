



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_event_sequence`

where not(event_sequence >= 1)

