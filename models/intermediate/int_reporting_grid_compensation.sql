/*
    Model:        int_reporting_grid_compensation
    Layer:        Intermediate — scaffold
    Materialized: view
    Grain:        report_month × department × sub_department × job_level
                  × level_group × gender × latest_perf_rating

    Purpose:
        Full month × dimension scaffold for fct_compensation_reporting.
        The mart LEFT JOINs aggregated salary / compa-ratio / band
        position onto this grid so every dimension cell is present in
        every month, even with zero employees in that cell.

    Construction:
        CROSS JOIN distinct (report_month, report_quarter) from
        dim_calendar with distinct compensation-dimension tuples from
        int_employee_monthly_roster. latest_perf_rating is the string
        description (e.g., "Exceeds Expectations") carried on the
        roster — NULL combinations (employees never reviewed) survive
        DISTINCT and appear as their own row in the scaffold.
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
        latest_perf_rating
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
        d.latest_perf_rating
    from calendar_months as c
    cross join dimension_combos as d

)

select * from final
