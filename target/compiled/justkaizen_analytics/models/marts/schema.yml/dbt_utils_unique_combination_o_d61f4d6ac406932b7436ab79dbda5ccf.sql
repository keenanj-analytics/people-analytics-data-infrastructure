





with validation_errors as (

    select
        survey_cycle, department, theme
    from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
    group by survey_cycle, department, theme
    having count(*) > 1

)

select *
from validation_errors


