



select
    1
from (select * from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup` where theme_favorable_pct_delta is not null) dbt_subquery

where not(theme_favorable_pct_delta between -1 and 1)

