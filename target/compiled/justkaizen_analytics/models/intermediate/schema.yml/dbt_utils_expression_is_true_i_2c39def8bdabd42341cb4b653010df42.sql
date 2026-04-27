



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_compensation_current`

where not(band_position between 0 and 1)

