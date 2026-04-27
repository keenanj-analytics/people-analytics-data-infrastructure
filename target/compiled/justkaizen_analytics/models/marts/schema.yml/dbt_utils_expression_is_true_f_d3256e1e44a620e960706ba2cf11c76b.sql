



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_attrition_drivers`

where not(tenure_at_termination_months >= 0)

