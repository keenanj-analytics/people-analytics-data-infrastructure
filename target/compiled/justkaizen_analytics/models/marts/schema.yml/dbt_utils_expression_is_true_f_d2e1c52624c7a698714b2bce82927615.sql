



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`

where not(theme_avg_score between 1 and 5)

