



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_tenure`

where not(career_velocity_per_year >= 0)

