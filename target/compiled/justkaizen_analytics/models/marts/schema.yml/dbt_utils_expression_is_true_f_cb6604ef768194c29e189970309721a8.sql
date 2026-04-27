



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`

where not(cohort_size >= 1)

