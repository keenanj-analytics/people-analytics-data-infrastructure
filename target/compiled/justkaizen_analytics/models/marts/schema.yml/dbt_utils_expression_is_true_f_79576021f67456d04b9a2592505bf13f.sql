



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`

where not(hires_in_period >= 0)

