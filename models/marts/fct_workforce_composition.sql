/*
    Model:        fct_workforce_composition
    Layer:        Mart
    Materialized: table
    Grain:        report_month × department × sub_department × job_level
                  × level_group × gender × race_ethnicity × manager_status
    Source:       int_reporting_grid_workforce (scaffold)
                  int_employee_monthly_roster (activity, Full Time only)

    Purpose:
        Headcount, hires, terminations, representation rates, span of
        control, and tenure across the workforce composition dimensions.
        No TTM rolling windows on this mart — workforce metrics are
        point-in-time per month.

    Construction:
        1. Aggregate the Full Time roster by 8 grid dimensions × month.
           Headcount counts active employees at month end. Terminations
           is ALL terminations in the month (NOT gated on
           is_attrition_eligible_term — workforce composition tracks
           every departure, including RIFs and end-of-contract).
        2. LEFT JOIN onto the workforce grid via IS NOT DISTINCT FROM,
           COALESCE NULLs to 0.
        3. Org-wide and department headcount per month, joined onto
           every cell to drive representation-rate denominators and the
           orgwide benchmarks.

    Notes:
        - All aggregations filter to employment_type = 'Full Time'.
        - terminations counts every termination, including excluded
          reasons (RIF, internships, etc.). fct_attrition_reporting
          uses is_attrition_eligible_term — these two marts will not
          agree on termination counts by design.
        - avg_span_of_control averages no_direct_reports only across
          rows where manager_status = TRUE. Cells where
          manager_status = FALSE return NULL (empty AVG over filtered
          set), which is the correct semantics — there's no span of
          control to report for non-managers.
        - gender_representation_rate and race_representation_rate share
          the same formula (cell headcount / department headcount) per
          the data dictionary. Different columns, same arithmetic in V1
          construction; semantics differ when the grid is sliced down
          to only one of the two demographics.
*/

with roster as (

    select * from {{ ref('int_employee_monthly_roster') }}

),

cell_aggregated as (

    select
        report_month,
        department,
        sub_department,
        job_level,
        level_group,
        gender,
        race_ethnicity,
        manager_status,
        countif(employment_status = 'Active')                                       as headcount,
        countif(date_trunc(hire_date, month) = report_month)                        as hires,
        countif(is_terminated_this_month)                                           as terminations,
        avg(case when manager_status then no_direct_reports end)                    as avg_span_of_control,
        avg(tenure_months)                                                          as avg_tenure_months
    from roster
    where employment_type = 'Full Time'
    group by 1, 2, 3, 4, 5, 6, 7, 8

),

scaffolded as (

    select
        g.report_month,
        g.report_quarter,
        g.department,
        g.sub_department,
        g.job_level,
        g.level_group,
        g.gender,
        g.race_ethnicity,
        g.manager_status,
        coalesce(a.headcount, 0)            as headcount,
        coalesce(a.hires, 0)                as hires,
        coalesce(a.terminations, 0)         as terminations,
        a.avg_span_of_control,
        a.avg_tenure_months
    from {{ ref('int_reporting_grid_workforce') }} as g
    left join cell_aggregated as a
        on  g.report_month     =                a.report_month
        and g.department       is not distinct from a.department
        and g.sub_department   is not distinct from a.sub_department
        and g.job_level        is not distinct from a.job_level
        and g.level_group      is not distinct from a.level_group
        and g.gender           is not distinct from a.gender
        and g.race_ethnicity   is not distinct from a.race_ethnicity
        and g.manager_status   is not distinct from a.manager_status

),

orgwide_monthly as (

    select
        report_month,
        countif(employment_status = 'Active')                                       as orgwide_headcount,
        avg(case when manager_status then no_direct_reports end)                    as orgwide_avg_span_of_control
    from roster
    where employment_type = 'Full Time'
    group by report_month

),

dept_monthly as (

    select
        report_month,
        department,
        countif(employment_status = 'Active')                                       as dept_headcount
    from roster
    where employment_type = 'Full Time'
    group by report_month, department

),

final as (

    select
        -- Time
        s.report_month,
        s.report_quarter,
        case
            when extract(month from s.report_month) in (3, 6, 9, 12)        then true
            when s.report_month = max(s.report_month) over ()               then true
            else false
        end                                                                 as flag_end_of_quarter,
        s.report_month = max(s.report_month) over ()                        as flag_latest_report,

        -- Dimensions
        s.department,
        s.sub_department,
        s.job_level,
        s.level_group,
        s.gender,
        s.race_ethnicity,
        s.manager_status,

        -- Cell-level metrics
        s.headcount,
        s.hires,
        s.terminations,
        s.hires - s.terminations                                            as net_change,
        safe_divide(s.headcount, o.orgwide_headcount)                       as pct_of_total_headcount,
        safe_divide(s.headcount, d.dept_headcount)                          as gender_representation_rate,
        safe_divide(s.headcount, d.dept_headcount)                          as race_representation_rate,
        s.avg_span_of_control,
        s.avg_tenure_months,

        -- Benchmarks
        o.orgwide_headcount,
        o.orgwide_avg_span_of_control

    from scaffolded as s
    left join orgwide_monthly as o
        on s.report_month = o.report_month
    left join dept_monthly as d
        on  s.report_month = d.report_month
        and s.department is not distinct from d.department

)

select * from final
