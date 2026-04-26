/*
    Model:        int_recruiting_funnel_metrics
    Layer:        intermediate
    Sources:      stg_recruiting
    Materialized: view  (per dbt_project.yml intermediate defaults)

    Purpose
    -------
    One row per requisition with funnel-stage volumes, per-stage
    conversion rates, and the spec's two time-to metrics. This is the
    natural granularity for the recruiting Tableau workbook (per-
    requisition velocity dashboard, time-to-fill by department, source
    mix per requisition).

    Granularity
    -----------
    One row per requisition_id. ~592 rows (one per ATS-era hire).

    Calculated fields
    -----------------
    Funnel stage counts (cumulative -- a candidate at Onsite is counted
    in reached_phone_screen, reached_onsite, but not reached_offer):
        total_applications        count(*) per requisition
        reached_phone_screen      count where phone_screen_date is not null
        reached_onsite            count where onsite_date is not null
        reached_offer             count where offer_date is not null
        count_hired               count where current_stage = 'Hired'
        count_rejected            count where current_stage = 'Rejected'
        count_withdrawn           count where current_stage = 'Withdrawn'

    Time-to metrics (per data dict, from the Hired candidate's timeline):
        time_to_fill_days   hire_date - hired_application_date
        time_to_offer_days  hired_offer_date - hired_application_date

    Stage conversion rates (decimals in [0, 1]):
        rate_applied_to_phone      reached_phone_screen / total_applications
        rate_phone_to_onsite       reached_onsite / reached_phone_screen
                                     (collapses Phone Screen -> Technical ->
                                      Onsite into one rate since Technical
                                      isn't tracked as a separate date)
        rate_onsite_to_offer       reached_offer / reached_onsite
        rate_offer_to_hired        count_hired / reached_offer
        rate_overall_conversion    count_hired / total_applications

    Spec target conversions (Section 9):
        Applied -> Phone Screen:           25%
        Phone Screen -> Technical:         45%   (not tracked separately)
        Technical -> Onsite:               55%
        Onsite -> Offer:                   40%
        Offer -> Hired:                    85%
        Overall Applied -> Hired:          ~2.1% (theoretical)

    The dataset's actual conversions are inflated above these targets
    because the Applied-stage rejection pool is capped during generation
    to keep total recruiting rows in the spec's 8-10K range. See
    11_build_raw_recruiting.py docstring for the trade-off.

    What this model does NOT compute
    --------------------------------
    Per-source funnel rates and source-level time-to metrics belong in
    a downstream mart (fct_recruiting_funnel_by_source) where they can
    be sliced for the dashboard. This model stays at requisition grain.
*/

{{ config(materialized='view') }}

with applications as (
    select * from {{ ref('stg_recruiting') }}
),

per_requisition as (
    select
        requisition_id,

        -- Requisition metadata (any_value -- all rows for a requisition
        -- carry the same values for these dimensions)
        any_value(department)     as department,
        any_value(sub_department) as sub_department,
        any_value(job_title)      as job_title,
        any_value(recruiter)      as recruiter,
        any_value(hiring_manager) as hiring_manager,

        -- Funnel stage volumes (cumulative)
        count(*)                                                              as total_applications,
        countif(phone_screen_date is not null)                                as reached_phone_screen,
        countif(onsite_date is not null)                                      as reached_onsite,
        countif(offer_date is not null)                                       as reached_offer,
        countif(current_stage = 'Hired')                                      as count_hired,
        countif(current_stage = 'Rejected')                                   as count_rejected,
        countif(current_stage = 'Withdrawn')                                  as count_withdrawn,

        -- Application-window markers
        min(application_date) as first_application_date,
        max(application_date) as last_application_date,

        -- Hired candidate's timeline (one Hired row per requisition by
        -- construction; max() over a single-row subset returns that row's value)
        max(case when current_stage = 'Hired' then application_date  end) as hired_application_date,
        max(case when current_stage = 'Hired' then phone_screen_date end) as hired_phone_screen_date,
        max(case when current_stage = 'Hired' then onsite_date       end) as hired_onsite_date,
        max(case when current_stage = 'Hired' then offer_date        end) as hired_offer_date,
        max(case when current_stage = 'Hired' then hire_date         end) as hire_date
    from applications
    group by requisition_id
),

final as (
    select
        *,

        -- Time-to metrics (per data dict)
        date_diff(hire_date,        hired_application_date, day) as time_to_fill_days,
        date_diff(hired_offer_date, hired_application_date, day) as time_to_offer_days,

        -- Stage conversion rates. SAFE_DIVIDE returns NULL on a 0
        -- denominator (defensive; in this dataset every requisition
        -- has at least one Hired row so denominators are >= 1).
        round(safe_divide(reached_phone_screen, total_applications),  4) as rate_applied_to_phone,
        round(safe_divide(reached_onsite,       reached_phone_screen),4) as rate_phone_to_onsite,
        round(safe_divide(reached_offer,        reached_onsite),      4) as rate_onsite_to_offer,
        round(safe_divide(count_hired,          reached_offer),       4) as rate_offer_to_hired,
        round(safe_divide(count_hired,          total_applications),  4) as rate_overall_conversion
    from per_requisition
)

select * from final
