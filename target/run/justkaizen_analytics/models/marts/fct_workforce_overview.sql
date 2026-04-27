
  
    

    create or replace table `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`
      
    
    

    
    OPTIONS(
      description="""Headline workforce time series. One row per (department,\nyear_quarter) across 21 quarters x 7 departments = 147 rows.\nCarries start / end headcount snapshots, period flow (hires,\nterminations split by type), net change, average period\nheadcount, and attrition rate. Q1 2023 layoff is visible in\nthe layoff_terminations column; the Section 1 hypergrowth peak\n(2022-Q2) is visible in hires_in_period.\n"""
    )
    as (
      /*
    Model:        fct_workforce_overview
    Layer:        marts
    Sources:      int_employee_dimension, stg_employees
    Materialized: table  (per dbt_project.yml marts defaults)

    Purpose
    -------
    Headline workforce time series for the executive dashboard. One row
    per (department, year_quarter). Period spans 2020-Q1 through 2025-Q1
    (21 quarters x 7 departments = 147 rows).

    Metrics
    -------
        start_headcount             active at the day before the quarter starts
        end_headcount               active at quarter_end_date
        net_change                  end - start
        hires_in_period             count of employees with hire_date in [start, end]
        terminations_in_period      count with termination_date in [start, end]
        voluntary_terminations
        involuntary_terminations
        layoff_terminations         (split of terminations_in_period by type)
        attrition_rate_pct          terminations / avg_period_headcount * 100
        avg_period_headcount        (start + end) / 2

    "Active on date X" = hire_date <= X AND (termination_date is null
    OR termination_date >= X). This treats termination_date as the
    employee's last day worked.

    Notable signals visible in this fact
    ------------------------------------
    - Q1 2023 layoff: 75 layoff_terminations across Sales (25),
      Engineering (20), G&A (12), Marketing (8), CS (5), People (5)
      with Product = 0 (Section 1 layoff distribution).
    - Hypergrowth peak: 2022-Q2 with ~80 hires_in_period across all
      departments (Section 1 headcount timeline).
    - Hiring freeze: 2023-Q1 / 2023-Q2 with 0 hires (post-layoff).
*/



with employees as (
    select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
),

quarters as (
    select
        quarter_start,
        date_sub(date_add(quarter_start, interval 1 quarter), interval 1 day) as quarter_end,
        date_sub(quarter_start, interval 1 day)                               as day_before_quarter,
        concat(
            cast(extract(year from quarter_start) as string),
            '-Q',
            cast(extract(quarter from quarter_start) as string)
        ) as year_quarter
    from unnest(
        generate_date_array(
            date '2020-01-01',
            date '2025-01-01',
            interval 1 quarter
        )
    ) as quarter_start
),

departments as (
    select distinct department
    from employees
    where department in (
        'Engineering', 'Sales', 'Customer Success', 'Marketing',
        'Product', 'G&A', 'People'
    )
),

quarter_dept_grid as (
    select
        q.year_quarter,
        q.quarter_start,
        q.quarter_end,
        q.day_before_quarter,
        d.department
    from quarters as q
    cross join departments as d
),

with_metrics as (
    select
        g.year_quarter,
        g.quarter_start,
        g.quarter_end,
        g.department,

        -- Headcount snapshots
        countif(
            e.department = g.department
            and e.hire_date <= g.day_before_quarter
            and (e.termination_date is null or e.termination_date >= g.day_before_quarter)
        ) as start_headcount,

        countif(
            e.department = g.department
            and e.hire_date <= g.quarter_end
            and (e.termination_date is null or e.termination_date >= g.quarter_end)
        ) as end_headcount,

        -- Period flow
        countif(
            e.department = g.department
            and e.hire_date between g.quarter_start and g.quarter_end
        ) as hires_in_period,

        countif(
            e.department = g.department
            and e.termination_date between g.quarter_start and g.quarter_end
        ) as terminations_in_period,

        countif(
            e.department = g.department
            and e.termination_date between g.quarter_start and g.quarter_end
            and e.termination_type = 'Voluntary'
        ) as voluntary_terminations,

        countif(
            e.department = g.department
            and e.termination_date between g.quarter_start and g.quarter_end
            and e.termination_type = 'Involuntary'
        ) as involuntary_terminations,

        countif(
            e.department = g.department
            and e.termination_date between g.quarter_start and g.quarter_end
            and e.termination_type = 'Layoff'
        ) as layoff_terminations
    from quarter_dept_grid as g
    left join employees       as e on true
    group by
        g.year_quarter,
        g.quarter_start,
        g.quarter_end,
        g.department
),

final as (
    select
        year_quarter,
        quarter_start                              as quarter_start_date,
        quarter_end                                as quarter_end_date,
        department,

        start_headcount,
        end_headcount,
        end_headcount - start_headcount            as net_change,
        round((start_headcount + end_headcount) / 2.0, 1) as avg_period_headcount,

        hires_in_period,
        terminations_in_period,
        voluntary_terminations,
        involuntary_terminations,
        layoff_terminations,

        round(
            safe_divide(
                terminations_in_period * 100.0,
                (start_headcount + end_headcount) / 2.0
            ),
            2
        ) as attrition_rate_pct
    from with_metrics
)

select * from final
order by quarter_start_date, department
    );
  