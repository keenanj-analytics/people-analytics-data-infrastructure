



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`

where not(total_applications >= 1)

