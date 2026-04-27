



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`

where not(start_headcount >= 0)

