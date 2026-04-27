



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_tenure`

where not(time_in_current_role_months >= 0)

