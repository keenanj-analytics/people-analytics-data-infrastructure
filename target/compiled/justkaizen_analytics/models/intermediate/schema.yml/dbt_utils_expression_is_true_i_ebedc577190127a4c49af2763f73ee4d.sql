



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`

where not(theme_avg_score between 1 and 5)

