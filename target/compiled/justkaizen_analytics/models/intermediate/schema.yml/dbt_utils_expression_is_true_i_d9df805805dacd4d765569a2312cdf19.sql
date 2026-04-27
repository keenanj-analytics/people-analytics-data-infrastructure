



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`

where not(rate_overall_conversion between 0 and 1)

