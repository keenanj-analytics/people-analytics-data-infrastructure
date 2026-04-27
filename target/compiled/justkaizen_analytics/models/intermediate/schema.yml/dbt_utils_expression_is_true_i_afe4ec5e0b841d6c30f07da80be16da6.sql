



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history`

where not(cycle_sequence >= 1)

