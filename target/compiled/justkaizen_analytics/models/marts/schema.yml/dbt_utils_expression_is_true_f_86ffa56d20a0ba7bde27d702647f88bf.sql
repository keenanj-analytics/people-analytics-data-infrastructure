



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`

where not(time_to_fill_days >= 0)

