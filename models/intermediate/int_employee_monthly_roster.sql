/*
    Model:        int_employee_monthly_roster
    Layer:        Intermediate — the golden record
    Materialized: view (default for the intermediate layer)
    Grain:        One row per employee per month they were active (or
                  terminated in that month)
    PK:           (employee_id, report_month)

    Purpose:
        The center of the warehouse. One row per employee per month with
        every reporting dimension and metric attached as columns. All
        four domain reporting marts (attrition, recruiting, workforce,
        compensation) aggregate from here. fct_employee_roster promotes
        this view directly for drill-through.

    Construction:
        1. CROSS JOIN deduplicated stg_employees (latest snapshot per
           employee) with distinct report_months from dim_calendar.
        2. Filter: employee appears in a month if
              hire_date <= last day of month
              AND (termination_date IS NULL OR termination_date >= start of month)
           — keeps terminated employees in their termination month and
           drops them after.
        3. LEFT JOIN int_employee_compensation_current,
           int_employee_performance_history, and a deduped stg_recruiting
           (latest hired record per employee).

    V1 design decisions:
        - Static (latest-known) values for compensation, performance, and
          all stg_employees attributes — including department, manager_id,
          salary, no_direct_reports, manager_status. Phase 2 introduces
          per-month point-in-time joins.
        - manager_status and no_direct_reports come from stg_employees
          (HRIS-provided) and are not recomputed via aggregation.

    Derived fields (computed in this model, not in any source):
        level_group, tenure_months, tenure_bucket, new_hire_flag,
        employment_status, termination_type, top_performer_flag,
        flag_latest_report, is_terminated_this_month,
        is_excluded_termination, is_attrition_eligible_term,
        is_rif_termination

    Notes:
        - Excluded termination reasons mirror CLAUDE.md exactly.
        - top_performer_flag layers in is_critical_talent (the OR clause
          deferred from int_employee_performance_history).
        - flag_latest_report uses MAX(report_month) OVER () across the
          entire roster — i.e., the most recent month containing any row.
        - tenure_bucket includes tenure_months = 0 in '0-1 Years', a
          minor deviation from CLAUDE.md's strict "0 < tenure_months"
          lower bound; brand-new hires belong in 0-1, not NULL.
*/

with calendar_months as (

    select distinct
        report_month,
        report_quarter
    from {{ ref('dim_calendar') }}
    where is_month_end

),

employees_latest as (

    -- Collapse multi-snapshot stg_employees to one row per employee
    -- using the most recent report_date.
    select *
    from (
        select
            *,
            row_number() over (
                partition by employee_id
                order by report_date desc
            ) as rn
        from {{ ref('stg_employees') }}
    )
    where rn = 1

),

employee_months as (

    -- Cross join + filter: each employee × every month they were active
    select
        e.employee_id,
        c.report_month,
        c.report_quarter,
        e.full_name,
        e.work_email,
        e.department,
        e.sub_department,
        e.team,
        e.job_title,
        e.job_level,
        e.employee_zone,
        e.manager_id,
        e.hire_date,
        e.termination_date,
        e.termination_voluntary,
        e.termination_reason,
        e.termination_regrettable,
        e.gender,
        e.race,
        e.work_state,
        e.work_country,
        e.critical_talent,
        e.employment_type,
        e.no_direct_reports,
        e.manager_status
    from employees_latest as e
    cross join calendar_months as c
    where e.hire_date <= last_day(c.report_month, month)
      and (
          e.termination_date is null
          or e.termination_date >= c.report_month
      )

),

recruiting as (

    -- One recruiting record per hired employee. Rehires (same person,
    -- multiple Hired requisitions over time) are resolved to the most
    -- recent by start_date.
    select
        employee_id,
        candidate_source,
        candidate_origin,
        candidate_recruiter,
        candidate_hiring_manager
    from (
        select
            employee_id,
            application_channel                 as candidate_source,
            origin                              as candidate_origin,
            recruiter                           as candidate_recruiter,
            hiring_manager                      as candidate_hiring_manager,
            row_number() over (
                partition by employee_id
                order by start_date desc nulls last
            ) as rn
        from {{ ref('stg_recruiting') }}
        where employee_id is not null
    )
    where rn = 1

),

enriched as (

    select
        em.employee_id,
        em.report_month,
        em.report_quarter,
        em.full_name,
        em.work_email                                                       as email,
        em.department,
        em.sub_department,
        em.team,
        em.job_title,
        em.job_level,
        case
            when em.job_level in ('P1','P2','P3')                then 'Junior IC'
            when em.job_level in ('P4','P5','P6')                then 'Senior IC'
            when em.job_level in ('M1','M2')                     then 'Manager'
            when em.job_level in ('M3','M4')                     then 'Director'
            when em.job_level in ('E1','E2','E3','E4','E5','E6') then 'Senior Leadership'
        end                                                                 as level_group,
        em.employee_zone,
        em.manager_id,
        em.hire_date,
        em.termination_date,
        case
            when em.termination_voluntary = true  then 'Voluntary'
            when em.termination_voluntary = false then 'Involuntary'
        end                                                                 as termination_type,
        em.termination_reason,
        em.termination_regrettable                                          as is_regrettable_termination,
        case
            when em.termination_date is null                              then 'Active'
            when date_trunc(em.termination_date, month) = em.report_month then 'Terminated'
            else 'Active'
        end                                                                 as employment_status,
        date_diff(last_day(em.report_month, month), em.hire_date, month)    as tenure_months,
        em.gender,
        em.race                                                             as race_ethnicity,
        em.work_state                                                       as location_state,
        em.work_country                                                     as location_country,
        em.critical_talent                                                  as is_critical_talent,
        em.employment_type,
        comp.salary,
        comp.comp_band_min,
        comp.comp_band_mid,
        comp.comp_band_max,
        comp.compa_ratio,
        perf.latest_perf_rating,
        perf.latest_perf_rating_numeric,
        rec.candidate_source,
        rec.candidate_origin,
        rec.candidate_recruiter,
        rec.candidate_hiring_manager,
        em.no_direct_reports,
        em.manager_status,
        coalesce(
            date_trunc(em.termination_date, month) = em.report_month,
            false
        )                                                                   as is_terminated_this_month,
        coalesce(
            em.termination_reason in (
                'Reduction in Force',
                'End of Contract',
                'Entity Change',
                'Acquisition/Merger',
                'End of Internship',
                'International Transfer',
                'Relocation',
                'Converting to FT'
            ),
            false
        )                                                                   as is_excluded_termination
    from employee_months as em
    left join {{ ref('int_employee_compensation_current') }} as comp
        on em.employee_id = comp.employee_id
    left join {{ ref('int_employee_performance_history') }} as perf
        on em.employee_id = perf.employee_id
    left join recruiting as rec
        on em.employee_id = rec.employee_id

),

final as (

    select
        -- Identifiers & time
        employee_id,
        report_month,
        report_quarter,

        -- Names
        full_name,
        email,

        -- Org
        department,
        sub_department,
        team,
        job_title,
        job_level,
        level_group,
        employee_zone,
        manager_id,

        -- Tenure / lifecycle
        hire_date,
        termination_date,
        termination_type,
        termination_reason,
        is_regrettable_termination,
        employment_status,
        tenure_months,
        case
            when tenure_months <= 12 then '0-1 Years'
            when tenure_months <= 24 then '1-2 Years'
            when tenure_months <= 36 then '2-3 Years'
            when tenure_months <= 48 then '3-4 Years'
            when tenure_months <= 60 then '4-5 Years'
            else '5+ Years'
        end                                                                 as tenure_bucket,
        case
            when tenure_months <= 12 then 'New Hire'
            else 'Tenured'
        end                                                                 as new_hire_flag,

        -- Demographics & location
        gender,
        race_ethnicity,
        location_state,
        location_country,
        is_critical_talent,
        employment_type,

        -- Compensation (latest-known per V1)
        salary,
        comp_band_min,
        comp_band_mid,
        comp_band_max,
        compa_ratio,

        -- Performance (latest-known per V1)
        latest_perf_rating,
        latest_perf_rating_numeric,
        case
            when coalesce(latest_perf_rating_numeric, 0) >= 4 then 'Y'
            when coalesce(is_critical_talent, false)          then 'Y'
            else 'N'
        end                                                                 as top_performer_flag,

        -- Recruiting context (only for hired employees)
        candidate_source,
        candidate_origin,
        candidate_recruiter,
        candidate_hiring_manager,

        -- Management
        no_direct_reports,
        manager_status,

        -- Latest-month flag (computed across the whole roster)
        case
            when report_month = max(report_month) over () then 'X'
        end                                                                 as flag_latest_report,

        -- Termination flags
        is_terminated_this_month,
        is_excluded_termination,
        is_terminated_this_month
            and not is_excluded_termination                                 as is_attrition_eligible_term,
        is_terminated_this_month
            and termination_reason = 'Reduction in Force'                   as is_rif_termination

    from enriched

)

select * from final
