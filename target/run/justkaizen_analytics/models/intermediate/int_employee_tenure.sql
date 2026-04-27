

  create or replace view `just-kaizen-ai`.`raw_intermediate`.`int_employee_tenure`
  OPTIONS(
      description="""Per-employee tenure and career-velocity calculations. One row per\nemployee. Reference date for \"current\" is var('current_date'),\ndefaulting to 2025-03-31 (the synthetic dataset's current\nquarter); for terminated employees the reference date is their\ntermination_date so tenure stops at departure.\n"""
    )
  as /*
    Model:        int_employee_tenure
    Layer:        intermediate
    Sources:      stg_employees, stg_job_history
    Materialized: view  (per dbt_project.yml intermediate defaults)

    Purpose
    -------
    One row per employee with all tenure / career-velocity calculations
    that downstream marts need. Centralizing these here means the math
    is defined once and consumed many times -- changes to the spec
    formulas only touch this file.

    Reference date
    --------------
    All calculations anchor on a "snapshot date" rather than wall-clock
    today, so the synthetic dataset's analytics stay stable as the real
    date drifts forward. The default snapshot is var('current_date')
    (2025-03-31, the spec's current quarter); for active employees the
    reference date used in calculations is the snapshot, for terminated
    employees it is their termination_date.

    Calculated fields
    -----------------
        tenure_months                   reference_date - hire_date in months
        tenure_years                    reference_date - hire_date in days / 365.25
        total_promotions                count of Promotion events
        last_promotion_date             max(effective_date where Promotion); null if never promoted
        months_since_last_promotion     reference_date - last_promotion_date; null if never promoted
        time_in_current_role_months     reference_date - max(effective_date across all change_types)
        last_manager_change_date        max(effective_date where Manager Change); null if no MC
        has_manager_change_last_12mo    bool: any MC within 12 months of reference_date
        career_velocity_per_year        total_promotions divided by tenure in years

    Edge cases
    ----------
    - Employees with no Promotion events: total_promotions = 0;
      last_promotion_date / months_since_last_promotion are NULL;
      career_velocity_per_year is 0.
    - Employees whose Hire date equals the reference date (tenure 0
      days): career_velocity_per_year is 0 (avoids divide-by-zero).
    - has_manager_change_last_12mo is false (not null) when no Manager
      Change has ever occurred for the employee.
*/



with employees as (
    select * from `just-kaizen-ai`.`raw_staging`.`stg_employees`
),

job_history as (
    select * from `just-kaizen-ai`.`raw_staging`.`stg_job_history`
),

snapshot as (
    select cast('2025-03-31' as date) as snapshot_date
),

-- Reference date per employee: termination_date if terminated, else snapshot.
employee_reference_date as (
    select
        employees.employee_id,
        employees.hire_date,
        employees.termination_date,
        coalesce(employees.termination_date, snapshot.snapshot_date) as reference_date
    from employees
    cross join snapshot
),

-- Promotion stats per employee.
promotion_stats as (
    select
        employee_id,
        count(*)                  as total_promotions,
        max(effective_date)       as last_promotion_date
    from job_history
    where change_type = 'Promotion'
    group by employee_id
),

-- Most recent Manager Change per employee.
manager_change_recency as (
    select
        employee_id,
        max(effective_date) as last_manager_change_date
    from job_history
    where change_type = 'Manager Change'
    group by employee_id
),

-- Most recent event of any change_type per employee. The Hire row
-- guarantees this is non-null for every employee.
last_event as (
    select
        employee_id,
        max(effective_date) as last_event_date
    from job_history
    group by employee_id
),

final as (
    select
        e.employee_id,

        -- Anchor dates
        e.hire_date,
        e.termination_date,
        e.reference_date,

        -- Tenure
        date_diff(e.reference_date, e.hire_date, month)            as tenure_months,
        round(date_diff(e.reference_date, e.hire_date, day) / 365.25, 2) as tenure_years,

        -- Promotion stats
        coalesce(ps.total_promotions, 0) as total_promotions,
        ps.last_promotion_date,
        case
            when ps.last_promotion_date is not null
                then date_diff(e.reference_date, ps.last_promotion_date, month)
            else null
        end as months_since_last_promotion,

        -- Time in current role (from most recent event of any kind).
        -- COALESCE on hire_date is defensive; the Hire event guarantees
        -- last_event_date is always populated.
        date_diff(
            e.reference_date,
            coalesce(le.last_event_date, e.hire_date),
            month
        ) as time_in_current_role_months,

        -- Manager change recency
        mcr.last_manager_change_date,
        case
            when mcr.last_manager_change_date is not null
                and date_diff(e.reference_date, mcr.last_manager_change_date, month) <= 12
                then true
            else false
        end as has_manager_change_last_12mo,

        -- Career velocity (promotions per tenure-year). Avoid
        -- divide-by-zero for same-day hires.
        case
            when date_diff(e.reference_date, e.hire_date, day) > 0
                then round(
                    coalesce(ps.total_promotions, 0)
                    * 365.25
                    / date_diff(e.reference_date, e.hire_date, day),
                    3
                )
            else 0
        end as career_velocity_per_year
    from employee_reference_date as e
    left join promotion_stats        as ps  on e.employee_id = ps.employee_id
    left join manager_change_recency as mcr on e.employee_id = mcr.employee_id
    left join last_event             as le  on e.employee_id = le.employee_id
)

select * from final;

