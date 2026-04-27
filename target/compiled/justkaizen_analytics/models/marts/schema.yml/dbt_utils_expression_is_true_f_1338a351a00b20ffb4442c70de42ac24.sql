



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`

where not(overall_rating_numeric between 1 and 5)

