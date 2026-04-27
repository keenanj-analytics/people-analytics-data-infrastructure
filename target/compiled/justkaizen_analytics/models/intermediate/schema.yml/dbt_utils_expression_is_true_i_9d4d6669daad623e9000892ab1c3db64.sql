



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history`

where not(manager_rating_numeric between 1 and 5)

