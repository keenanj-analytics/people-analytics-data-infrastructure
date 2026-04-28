/*
    Model:        int_monthly_headcount_snapshot
    Layer:        intermediate
    Sources:      int_employee_dimension
    Materialized: view  (per dbt_project.yml intermediate defaults)

    Purpose
    -------
    Monthly headcount fact at employee grain. One row per employee per
    month they were on the company's books. Powers headcount-over-time,
    attrition, cohort, and demographic-trend marts so consumers don't
    each redo the date arithmetic.

    Granularity
    -----------
    One row per (employee_id, snapshot_month). snapshot_month is the
    first day of the calendar month.

    Date range
    ----------
    Jan 2020 through Mar 2025 inclusive (63 months). Anchored at the
    company's Q1-2020 founding (the floor of every hire_date) and
    capped at var('current_date') = 2025-03-31 for parity with the
    rest of the warehouse.

    Membership rule
    ---------------
    A row is emitted for (employee, month) if the employee was on
    payroll for any part of the calendar month:
        hire_date        <= last day of month
        termination_date >= first day of month  (or null = still active)
    By construction mid-month hires / terminations are counted in
    their hire / termination month -- the standard HR headcount
    convention.

    Carry-forward semantics (IMPORTANT)
    -----------------------------------
    All carry-forward attributes come from int_employee_dimension and
    therefore reflect the employee's CURRENT state, not the at-snapshot
    state:
        department, sub_department, job_level
            Current org placement. Point-in-time attribution via
            stg_job_history is intentionally deferred to a future SCD2
            model; for trending the simplification is acceptable since
            most employees don't change department mid-tenure.
        location_state, gender, race_ethnicity
            Slowly-changing demographics; carry-forward is faithful.
        is_top_performer
            Renamed from is_currently_top_performer for column parity.
            Reflects the LATEST review cycle, not the rating in effect
            at snapshot_month. Null for employees with no review
            history.
        tenure_band
            Current tenure band, not band-as-of-snapshot. Consumers
            that need point-in-time tenure should compute it from
            (snapshot_month - hire_date) directly.
        is_active
            True for never-terminated employees across every snapshot
            row; false for since-terminated employees across every row
            (including months they were active). The membership filter
            already excludes post-termination months.
*/

{{ config(materialized='view') }}

with month_spine as (
    select snapshot_month
    from unnest(
        generate_date_array(date '2020-01-01', date '2025-03-01', interval 1 month)
    ) as snapshot_month
),

employees as (
    select * from {{ ref('int_employee_dimension') }}
),

monthly_snapshot as (
    select
        e.employee_id,
        m.snapshot_month,

        -- Carry-forward attributes from int_employee_dimension
        e.department,
        e.sub_department,
        e.job_level,
        e.location_state,
        e.gender,
        e.race_ethnicity,
        e.tenure_band,
        e.is_currently_top_performer as is_top_performer,
        e.is_active
    from employees   as e
    cross join month_spine as m
    where e.hire_date        <= last_day(m.snapshot_month, month)
      and (e.termination_date is null or e.termination_date >= m.snapshot_month)
)

select * from monthly_snapshot
