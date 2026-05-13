/*
    Model:        fct_compensation_reporting
    Layer:        Mart
    Materialized: table
    Grain:        report_month × department × sub_department × job_level
                  × level_group × gender × race_ethnicity
                  × latest_perf_rating
    Source:       int_reporting_grid_compensation (scaffold)
                  int_employee_monthly_roster (Full Time, all statuses)

    Purpose:
        Salary, compa-ratio, band-position, and band-position outliers
        sliced across the compensation dimensions. No TTM rolling
        windows — compensation metrics are point-in-time per month.

    Construction:
        1. Aggregate active Full Time roster rows by 7 grid dimensions
           × month: employee_count, avg salary / compa_ratio /
           band_position, count below / above band, median compa_ratio.
        2. LEFT JOIN onto the compensation grid via IS NOT DISTINCT FROM.
           Cells with no employees stay NULL on averages and 0 on counts
           (median is NULL — APPROX_QUANTILES of an empty set yields
           NULL, intentionally not zero-padded since "median of nothing"
           ≠ 0).
        3. Org-wide and per-department averages (compa_ratio, salary)
           per month, joined onto every cell as benchmarks.

    Compensation flags:
        - count_below_band: compa_ratio < 0.90
        - count_above_band: compa_ratio > 1.10
        - In-band ([0.90, 1.10]) is the implicit complement.

    avg_band_position formula:
        (salary - comp_band_min) / (comp_band_max - comp_band_min)
        SAFE_DIVIDE handles roles where min = max (no band range);
        AVG over the cell averages each employee's position.

    Notes:
        - Aggregations filter to employment_type = 'Full Time'.
          employment_status is included as a dimension so Tableau can
          slice active vs. terminated. race_ethnicity added for pay
          equity analysis across demographic groups.
        - latest_perf_rating is the string description from the roster
          ("Exceeds Expectations" etc.); NULL for never-reviewed employees.
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
        employment_status,
        latest_perf_rating,
        count(*)                                                                    as employee_count,
        avg(salary)                                                                 as avg_salary,
        avg(compa_ratio)                                                            as avg_compa_ratio,
        avg(safe_divide(salary - comp_band_min, comp_band_max - comp_band_min))     as avg_band_position,
        countif(compa_ratio < 0.90)                                                 as count_below_band,
        countif(compa_ratio > 1.10)                                                 as count_above_band,
        approx_quantiles(compa_ratio, 2) [offset(1)]                                as median_compa_ratio
    from roster
    where employment_type = 'Full Time'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9

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
        a.employment_status,
        g.latest_perf_rating,
        coalesce(a.employee_count, 0)       as employee_count,
        a.avg_salary,
        a.avg_compa_ratio,
        a.avg_band_position,
        coalesce(a.count_below_band, 0)     as count_below_band,
        coalesce(a.count_above_band, 0)     as count_above_band,
        a.median_compa_ratio
    from {{ ref('int_reporting_grid_compensation') }} as g
    left join cell_aggregated as a
        on  g.report_month        =                a.report_month
        and g.department          is not distinct from a.department
        and g.sub_department      is not distinct from a.sub_department
        and g.job_level           is not distinct from a.job_level
        and g.level_group         is not distinct from a.level_group
        and g.gender              is not distinct from a.gender
        and g.race_ethnicity      is not distinct from a.race_ethnicity
        and g.latest_perf_rating  is not distinct from a.latest_perf_rating

),

orgwide_monthly as (

    select
        report_month,
        avg(compa_ratio)        as orgwide_avg_compa_ratio,
        avg(salary)             as orgwide_avg_salary
    from roster
    where employment_type = 'Full Time'
      and employment_status = 'Active'
    group by report_month

),

dept_monthly as (

    select
        report_month,
        department,
        avg(compa_ratio)        as dept_avg_compa_ratio
    from roster
    where employment_type = 'Full Time'
      and employment_status = 'Active'
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
        s.employment_status,
        s.latest_perf_rating,

        -- Cell-level metrics
        s.employee_count,
        s.avg_salary,
        s.avg_compa_ratio,
        s.avg_band_position,
        s.count_below_band,
        s.count_above_band,
        s.median_compa_ratio,

        -- Benchmarks
        o.orgwide_avg_compa_ratio,
        o.orgwide_avg_salary,
        d.dept_avg_compa_ratio

    from scaffolded as s
    left join orgwide_monthly as o
        on s.report_month = o.report_month
    left join dept_monthly as d
        on  s.report_month = d.report_month
        and s.department is not distinct from d.department

)

select * from final
