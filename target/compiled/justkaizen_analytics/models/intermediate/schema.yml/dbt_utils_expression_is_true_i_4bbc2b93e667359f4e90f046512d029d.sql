



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`

where not(rate_phone_to_onsite between 0 and 1)

