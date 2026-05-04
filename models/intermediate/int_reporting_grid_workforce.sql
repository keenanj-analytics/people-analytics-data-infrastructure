/*
    Model:        int_reporting_grid_workforce
    Layer:        Intermediate — scaffold
    Materialized: view
    Grain:        report_month × department × sub_department × job_level
                  × level_group × gender × race_ethnicity × manager_status

    Purpose:
        Full month × dimension scaffold for fct_workforce_composition.
        The mart LEFT JOINs aggregated headcount, hires, terminations,
        and span-of-control metrics onto this grid so every cell is
        present in every month, even with zero activity.

    Construction:
        CROSS JOIN distinct (report_month, report_quarter) from
        dim_calendar with distinct workforce-dimension tuples from
        int_employee_monthly_roster.
*/

with calendar_months as (

    select distinct
        report_month,
        report_quarter
    from {{ ref('dim_calendar') }}
    where is_month_end

),

dimension_combos as (

    select distinct
        department,
        sub_department,
        job_level,
        level_group,
        gender,
        race_ethnicity,
        manager_status
    from {{ ref('int_employee_monthly_roster') }}

),

final as (

    select
        c.report_month,
        c.report_quarter,
        d.department,
        d.sub_department,
        d.job_level,
        d.level_group,
        d.gender,
        d.race_ethnicity,
        d.manager_status
    from calendar_months as c
    cross join dimension_combos as d

)

select * from final
