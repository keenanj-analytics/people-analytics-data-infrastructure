/*
    Model:        fct_performance_distribution
    Layer:        marts
    Sources:      int_employee_performance_history, int_employee_dimension
    Materialized: table  (per dbt_project.yml marts defaults)

    Purpose
    -------
    Per (employee, review_cycle) wide row with all dims needed to
    slice the performance distribution: dept, level, demographics,
    tenure, comp. Granularity matches int_employee_performance_history
    (~2,676 rows).

    Caveat: dept / level / comp dimensions are CURRENT (snapshot from
    int_employee_dimension), not at-time-of-cycle. For most dashboards
    this is the right framing -- analysts ask "what's the rating
    distribution for current Senior Software Engineers" rather than
    "...for who was a Senior Software Engineer in 2022-H2". A true
    historical snapshot would require pivoting stg_job_history to a
    state-at-date function (deferred).

    Calculated fields (added beyond int_employee_performance_history)
    ----------------------------------------------------------------
        cycle_year                    extract(year from cycle_end_date)
        cycle_half                    'H1' or 'H2'
        rating_distribution_quartile  bucket of overall_rating_numeric for
                                      easy bar-chart bucketing in BI
*/

{{ config(materialized='table') }}

with performance as (
    select * from {{ ref('int_employee_performance_history') }}
),

employees as (
    select * from {{ ref('int_employee_dimension') }}
),

final as (
    select
        -- Cycle identity
        p.review_cycle,
        p.cycle_end_date,
        extract(year from p.cycle_end_date) as cycle_year,
        case
            when p.review_cycle like '%-H1' then 'H1'
            else 'H2'
        end as cycle_half,
        p.cycle_sequence,
        p.review_completed_date,
        p.review_status,

        -- Employee identity
        p.employee_id,
        e.first_name,
        e.last_name,

        -- Org context (CURRENT, not at-time-of-cycle)
        e.department,
        e.sub_department,
        e.job_level,
        e.job_title,
        e.manager_id,
        e.is_active,

        -- Demographics
        e.gender,
        e.race_ethnicity,
        e.location_state,
        e.is_critical_talent,

        -- Tenure context at the snapshot date
        e.tenure_years,
        e.total_promotions,
        e.career_velocity_per_year,

        -- Compensation context
        e.salary,
        e.compa_ratio,
        e.band_position,

        -- Ratings
        p.overall_rating,
        p.overall_rating_numeric,
        p.manager_rating,
        p.manager_rating_numeric,
        p.self_rating,
        p.self_rating_numeric,

        -- Trend signals
        p.prior_overall_rating,
        p.prior_overall_rating_numeric,
        p.overall_rating_delta,
        p.is_top_performer,
        p.self_optimism_delta,

        -- Bar-chart bucket
        case p.overall_rating
            when 'Significantly Exceeds' then 'High'
            when 'Exceeds'                then 'High'
            when 'Meets'                  then 'On Target'
            when 'Partially Meets'        then 'Below'
            when 'Does Not Meet'          then 'Below'
        end as rating_distribution_band
    from performance as p
    left join employees as e on p.employee_id = e.employee_id
)

select * from final
