/*
    Model:   int_employee_tenure
    Layer:   Intermediate (helper)
    Sources: stg_employees, stg_job_history
    Grain:   One row per employee
    PK:      employee_id

    Purpose:
        Per-employee tenure and career velocity. Feeds the roster as a
        helper. The roster recomputes tenure_months per report_month for
        time-grain accuracy; this model exposes a single "current" tenure
        value alongside promotion counts and pace.

    Logic:
        - as_of_date = MAX(report_date) across stg_employees. Using the
          latest snapshot rather than CURRENT_DATE() avoids negative
          tenure if the synthetic data extends past today.
        - tenure_months = DATE_DIFF(COALESCE(termination_date, as_of_date),
          hire_date, MONTH).
        - total_promotions = COUNT of stg_job_history rows where
          change_type = 'Promotion'. Employees absent from stg_job_history
          get 0 via COALESCE.
        - career_velocity_per_year = total_promotions / tenure_years,
          rounded to 2dp, with SAFE_DIVIDE for zero/null tenure.

    Notes:
        - stg_employees carries one row per (employee, report_date).
          Deduplicated via ANY_VALUE(hire_date) and MAX(termination_date)
          — hire_date is invariant per employee; MAX picks up the
          termination once it appears in a later snapshot.
        - change_type = 'Promotion' is the assumed value emitted by the
          synthetic generator. Align with the generator during the
          rescale phase if a different label is used.
*/

with as_of_date as (

    select max(report_date) as as_of_date
    from {{ ref('stg_employees') }}

),

employees as (

    -- Collapse multi-snapshot stg_employees to one row per employee
    select
        employee_id,
        any_value(hire_date) as hire_date,
        max(termination_date) as termination_date
    from {{ ref('stg_employees') }}
    group by employee_id

),

promotions as (

    select
        employee_id,
        count(*) as total_promotions
    from {{ ref('stg_job_history') }}
    where change_type = 'Promotion'
    group by employee_id

),

joined as (

    select
        e.employee_id,
        e.hire_date,
        e.termination_date,
        date_diff(
            coalesce(e.termination_date, a.as_of_date),
            e.hire_date,
            month
        ) as tenure_months,
        coalesce(p.total_promotions, 0) as total_promotions
    from employees as e
    cross join as_of_date as a
    left join promotions as p
        on e.employee_id = p.employee_id

),

final as (

    select
        employee_id,
        hire_date,
        termination_date,
        tenure_months,
        total_promotions,
        round(
            safe_divide(total_promotions, tenure_months / 12.0),
            2
        ) as career_velocity_per_year
    from joined

)

select * from final
