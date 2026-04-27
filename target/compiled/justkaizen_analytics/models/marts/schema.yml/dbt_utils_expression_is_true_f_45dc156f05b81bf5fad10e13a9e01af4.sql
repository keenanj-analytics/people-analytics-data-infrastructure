



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`

where not(end_headcount >= 0)

