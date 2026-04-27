



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`

where not(terminations_in_period >= 0)

