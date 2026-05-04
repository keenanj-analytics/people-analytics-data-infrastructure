/*
    Model:        int_reporting_grid_recruiting
    Layer:        Intermediate — scaffold
    Materialized: view
    Grain:        report_month × department × sub_department × job_level
                  × candidate_source × candidate_origin
                  × candidate_recruiter × candidate_hiring_manager

    Purpose:
        Full month × dimension scaffold for fct_recruiting_reporting. The
        mart LEFT JOINs aggregated hire / offer counts and time-to-fill
        onto this grid so every cell — even months with zero hires for a
        given recruiter / channel combo — has a row. Keeps trend lines
        unbroken and TTM rolling-window denominators stable.

    Construction:
        CROSS JOIN distinct (report_month, report_quarter) from
        dim_calendar with distinct recruiting-dimension tuples from
        int_employee_monthly_roster. The roster carries these fields
        for hired employees only, so combinations that exist purely on
        unfilled requisitions in stg_recruiting (e.g., a recruiter who
        ran reqs but landed no hires) will be absent from the scaffold.
        Acceptable trade-off for V1; expand the source to include
        stg_recruiting directly if those slices become material.
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
        candidate_source,
        candidate_origin,
        candidate_recruiter,
        candidate_hiring_manager
    from {{ ref('int_employee_monthly_roster') }}

),

final as (

    select
        c.report_month,
        c.report_quarter,
        d.department,
        d.sub_department,
        d.job_level,
        d.candidate_source,
        d.candidate_origin,
        d.candidate_recruiter,
        d.candidate_hiring_manager
    from calendar_months as c
    cross join dimension_combos as d

)

select * from final
