/*
    Model:        int_employee_dimension
    Layer:        intermediate
    Sources:      stg_employees
                  int_employee_tenure
                  int_employee_compensation_current
                  int_employee_performance_history
    Materialized: view  (per dbt_project.yml intermediate defaults)

    Purpose
    -------
    Wide single-row-per-employee dimension combining the headline
    attributes most marts need to join on. Pre-joins:
      - Employee identity / org placement / demographics from stg_employees
      - Tenure metrics from int_employee_tenure
      - Current compensation + compa_ratio + band_position from
        int_employee_compensation_current
      - Latest review cycle outcome from int_employee_performance_history

    Marts can join one row per employee here instead of repeating four
    joins (and their nullability handling) in every mart that needs the
    full picture.

    Joins are LEFT JOINs because:
      - Tenure: every employee has a row, but defensive
      - Comp: every employee has at least one comp row by Section 12 HR6,
        but defensive
      - Latest performance: not every employee has reviews (2025-Q1
        hires and very-short-tenure Early Churners). Performance fields
        are intentionally nullable for those employees.

    The is_active boolean is added as a convenience -- almost every mart
    filters on it, so deriving it once here and exposing it as a typed
    bool keeps downstream SQL cleaner than re-checking employment_status.
*/

{{ config(materialized='view') }}

with employees as (
    select * from {{ ref('stg_employees') }}
),

tenure as (
    select * from {{ ref('int_employee_tenure') }}
),

current_comp as (
    select * from {{ ref('int_employee_compensation_current') }}
),

latest_performance as (
    select *
    from {{ ref('int_employee_performance_history') }}
    qualify row_number() over (
        partition by employee_id
        order by cycle_sequence desc
    ) = 1
),

final as (
    select
        -- Identity
        e.employee_id,
        e.first_name,
        e.last_name,
        e.email,

        -- Org placement
        e.department,
        e.sub_department,
        e.job_title,
        e.job_level,
        e.manager_id,

        -- Employment timeline
        e.hire_date,
        e.termination_date,
        e.employment_status,
        case when e.employment_status = 'Active' then true else false end as is_active,
        e.termination_type,
        e.termination_reason,

        -- Demographics
        e.gender,
        e.race_ethnicity,
        e.location_city,
        e.location_state,

        -- Talent designation
        e.is_critical_talent,

        -- Tenure (from int_employee_tenure)
        t.reference_date,
        t.tenure_months,
        t.tenure_years,
        case
            when t.tenure_years < 1 then '0-1 years'
            when t.tenure_years < 2 then '1-2 years'
            when t.tenure_years < 3 then '2-3 years'
            when t.tenure_years < 4 then '3-4 years'
            when t.tenure_years < 5 then '4-5 years'
            else '5+ years'
        end as tenure_band,
        t.total_promotions,
        t.last_promotion_date,
        t.months_since_last_promotion,
        t.time_in_current_role_months,
        t.last_manager_change_date,
        t.has_manager_change_last_12mo,
        t.career_velocity_per_year,

        -- Current compensation (from int_employee_compensation_current)
        c.salary,
        c.comp_band_min,
        c.comp_band_mid,
        c.comp_band_max,
        c.compa_ratio,
        c.band_position,
        c.current_comp_change_reason,
        c.current_comp_effective_date,

        -- Latest performance review (from int_employee_performance_history)
        p.review_cycle           as latest_review_cycle,
        p.cycle_end_date         as latest_review_cycle_end_date,
        p.overall_rating         as latest_overall_rating,
        p.overall_rating_numeric as latest_overall_rating_numeric,
        p.review_status          as latest_review_status,
        p.is_top_performer       as is_currently_top_performer
    from employees                  as e
    left join tenure                as t on e.employee_id = t.employee_id
    left join current_comp          as c on e.employee_id = c.employee_id
    left join latest_performance    as p on e.employee_id = p.employee_id
)

select * from final
