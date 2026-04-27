
    
    

with all_values as (

    select
        theme as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`
    group by theme

)

select *
from all_values
where value_field not in (
    'Employee Engagement','Manager Effectiveness','Career Growth & Development','Work-Life Balance','Recognition','Company Culture','Communication','Resources & Enablement'
)


