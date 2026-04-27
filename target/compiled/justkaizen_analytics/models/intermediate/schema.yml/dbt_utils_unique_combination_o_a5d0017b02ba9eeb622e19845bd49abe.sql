





with validation_errors as (

    select
        survey_cycle, department, theme
    from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`
    group by survey_cycle, department, theme
    having count(*) > 1

)

select *
from validation_errors


