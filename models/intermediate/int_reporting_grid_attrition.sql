/*
    Model:        int_reporting_grid_attrition
    Layer:        Intermediate — scaffold
    Materialized: view
    Grain:        report_month × department × sub_department × job_level
                  × level_group × tenure_bucket × top_performer_flag
                  × gender × race_ethnicity

    Purpose:
        Full month × dimension scaffold for fct_attrition_reporting. The
        mart LEFT JOINs aggregated termination counts and headcount onto
        this grid so every cell — even ones with zero activity — has a
        row. This is what prevents gaps in trend lines and keeps the TTM
        rolling-window calculations correct (denominators don't disappear
        in months with zero activity).

    Construction:
        CROSS JOIN distinct (report_month, report_quarter) from
        dim_calendar with distinct dimension tuples from
        int_employee_monthly_roster. Only combinations that have ever had
        at least one employee in the roster appear here.
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
        tenure_bucket,
        top_performer_flag,
        gender,
        race_ethnicity
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
        d.tenure_bucket,
        d.top_performer_flag,
        d.gender,
        d.race_ethnicity
    from calendar_months as c
    cross join dimension_combos as d

)

select * from final
