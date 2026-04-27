



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`

where not(count_hired = 1)

