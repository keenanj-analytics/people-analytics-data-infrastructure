



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`

where not(theme_avg_favorable_pct between 0 and 1)

