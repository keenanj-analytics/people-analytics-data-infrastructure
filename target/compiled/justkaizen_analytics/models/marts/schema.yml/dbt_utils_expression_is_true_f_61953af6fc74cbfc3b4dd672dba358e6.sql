



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`

where not(compa_ratio_percentile_within_dept_level between 0 and 1)

