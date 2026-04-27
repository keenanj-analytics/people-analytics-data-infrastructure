



select
    1
from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`

where not(questions_in_theme between 3 and 5)

