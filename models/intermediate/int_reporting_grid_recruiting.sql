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
        dim_calendar with distinct recruiting-dimension tuples sourced
        from two places:
          1. int_employee_monthly_roster — covers hired candidates (always
             have department, sub_department, job_level populated).
          2. stg_recruiting + stg_comp_bands + requisition hire lookup —
             covers non-hired candidates (declined offers, archived).
             Department and job_level from comp_bands via job title;
             sub_department from the hired candidate on the same
             requisition. Unfilled reqs keep sub_department = NULL.
        The UNION ensures both hired and non-hired dimension combos exist
        in the scaffold.
*/

with calendar_months as (

    select distinct
        report_month,
        report_quarter
    from {{ ref('dim_calendar') }}
    where is_month_end

),

-- Dimension combos from hired employees (roster-based, always has sub_department)
roster_combos as (

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

-- Sub-department for each requisition, derived from the hired candidate
req_sub_dept as (

    select distinct
        r.requisition_id,
        e.sub_department            as req_sub_department
    from {{ ref('stg_recruiting') }} as r
    inner join (
        select distinct employee_id, sub_department
        from {{ ref('int_employee_monthly_roster') }}
    ) as e on r.employee_id = e.employee_id
    where r.employee_id is not null

),

-- Dimension combos from all candidates (including non-hired)
-- Department/job_level from comp_bands; sub_department from requisition's hire
candidate_combos as (

    select distinct
        cb.department,
        req.req_sub_department      as sub_department,
        cb.job_level,
        r.Source                    as candidate_source,
        r.Origin                   as candidate_origin,
        r.Recruiter                as candidate_recruiter,
        r.Hiring_Manager           as candidate_hiring_manager
    from {{ source('raw', 'raw_offers_hires') }} as r
    left join {{ ref('stg_comp_bands') }} as cb
        on r.Job = cb.job_title
    left join req_sub_dept as req
        on r.Requisition_ID = req.requisition_id

),

dimension_combos as (

    select * from roster_combos
    union distinct
    select * from candidate_combos

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
