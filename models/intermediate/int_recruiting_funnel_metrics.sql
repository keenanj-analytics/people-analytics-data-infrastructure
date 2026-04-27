/*
    Model:        int_recruiting_funnel_metrics
    Layer:        intermediate
    Sources:      stg_recruiting
    Materialized: view  (per dbt_project.yml intermediate defaults)

    Purpose
    -------
    One row per requisition with funnel-stage volumes, per-stage
    conversion rates, the spec's two time-to metrics, and the dominant
    application channel ("source") for each requisition. This is the
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

    Top application channel (source mix, per requisition):
        top_application_channel        most-common stg_recruiting.source
                                       value across the requisition's
                                       applications, ties broken
                                       alphabetically.
        top_application_channel_count  count of applications attributed
                                       to top_application_channel.
        top_application_channel_share  count / total_applications.

    Naming note
    -----------
    The seed `source` column is aliased to `application_channel`
    upstream in stg_recruiting. BigQuery treats the bare identifier
    `source` as reserved (`MERGE INTO target USING source ...` syntax)
    and the parser misreads it in GROUP BY / ORDER BY / PARTITION BY
    positions as a row-as-struct expression, surfacing as
        "Ordering by expressions of type STRUCT is not allowed".
    Aliasing once at the staging boundary keeps every downstream
    model (intermediate, marts, analyses) bare-source-free.

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
*/

{{ config(materialized='view') }}

with per_requisition as (
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
    from {{ ref('stg_recruiting') }}
    group by requisition_id
),

-- Per (requisition, channel) application counts.
channel_counts_per_req as (
    select
        requisition_id,
        application_channel,
        count(*) as channel_count
    from {{ ref('stg_recruiting') }}
    group by requisition_id, application_channel
),

-- Pick the dominant channel per requisition (ties broken alphabetically).
top_channel_per_req as (
    select
        requisition_id,
        application_channel as top_application_channel,
        channel_count       as top_application_channel_count
    from channel_counts_per_req
    qualify row_number() over (
        partition by requisition_id
        order by channel_count desc, application_channel
    ) = 1
),

final as (
    select
        pr.*,

        -- Time-to metrics (per data dict)
        date_diff(pr.hire_date,        pr.hired_application_date, day) as time_to_fill_days,
        date_diff(pr.hired_offer_date, pr.hired_application_date, day) as time_to_offer_days,

        -- Stage conversion rates. SAFE_DIVIDE returns NULL on a 0
        -- denominator (defensive; in this dataset every requisition
        -- has at least one Hired row so denominators are >= 1).
        round(safe_divide(pr.reached_phone_screen, pr.total_applications),  4) as rate_applied_to_phone,
        round(safe_divide(pr.reached_onsite,       pr.reached_phone_screen),4) as rate_phone_to_onsite,
        round(safe_divide(pr.reached_offer,        pr.reached_onsite),      4) as rate_onsite_to_offer,
        round(safe_divide(pr.count_hired,          pr.reached_offer),       4) as rate_offer_to_hired,
        round(safe_divide(pr.count_hired,          pr.total_applications),  4) as rate_overall_conversion,

        -- Top channel
        tc.top_application_channel,
        tc.top_application_channel_count,
        round(safe_divide(tc.top_application_channel_count, pr.total_applications), 4) as top_application_channel_share
    from per_requisition       as pr
    left join top_channel_per_req as tc on pr.requisition_id = tc.requisition_id
)

select * from final
