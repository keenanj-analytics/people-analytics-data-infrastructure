



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`

where not(reached_onsite >= 1)

