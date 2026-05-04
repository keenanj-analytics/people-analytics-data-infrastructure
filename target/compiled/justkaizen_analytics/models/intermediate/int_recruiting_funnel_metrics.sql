/*
    Model:  int_recruiting_funnel_metrics
    Layer:  Intermediate
    Source: stg_recruiting
    Grain:  One row per requisition_id

    Purpose:
        Per-requisition funnel rollup. Counts candidates at each stage
        of the pipeline, derives stage-to-stage conversion rates, and
        surfaces a single time_to_fill_days, offer_acceptance_rate, and
        top_source per requisition. Feeds fct_recruiting_velocity for
        the per-req drill-through table in Tableau.

    Stage ordinals (from Furthest_Stage_Reached):
        1 Applied -> 2 Recruiter Screen -> 3 Phone Screen
        -> 4 Technical -> 5 Onsite -> 6 Offer -> 7 Hired

    Funnel cuts:
        - total_applicants = all candidates on the req
        - total_screened   = made it past Applied (>= Recruiter Screen)
        - total_interviewed = reached Phone Screen or beyond
        - total_offered     = reached Offer or Hired
        - total_hired       = outcome = 'Hired'
        - total_declined    = reached Offer but never made Hired

    Notes:
        - time_to_fill_days lives on every candidate row of the req but
          is only populated for the hired candidate (it derives from
          their start_date). MAX() picks the hired candidate's value.
        - top_source uses APPROX_TOP_COUNT for clarity over an exact
          window-function rank; tie-breaking is non-deterministic but
          immaterial for V1 reporting.
        - This model has no roster dependency — recruiting is a
          separate path. job_title here comes from the requisition's
          Job field (stg_recruiting.job_title), not from comp_bands.
*/

with recruiting as (

    select * from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`

),

with_stage_ordinal as (

    select
        *,
        case furthest_stage_reached
            when 'Applied'          then 1
            when 'Recruiter Screen' then 2
            when 'Phone Screen'     then 3
            when 'Technical'        then 4
            when 'Onsite'           then 5
            when 'Offer'            then 6
            when 'Hired'            then 7
        end as stage_ordinal
    from recruiting

),

aggregated as (

    select
        requisition_id,
        any_value(job_title)                    as job_title,
        any_value(recruiter)                    as recruiter,
        any_value(hiring_manager)               as hiring_manager,
        any_value(requisition_fill_start_date)  as requisition_fill_start_date,

        count(*)                                                                    as total_applicants,
        countif(stage_ordinal >= 2)                                                 as total_screened,
        countif(stage_ordinal >= 3)                                                 as total_interviewed,
        countif(stage_ordinal >= 6)                                                 as total_offered,
        countif(outcome = 'Hired')                                                  as total_hired,
        countif(stage_ordinal = 6 and outcome <> 'Hired')                           as total_declined,

        max(time_to_fill_days)                                                      as time_to_fill_days,
        approx_top_count(application_channel, 1) [offset(0)].value                  as top_source

    from with_stage_ordinal
    group by requisition_id

),

final as (

    select
        requisition_id,
        job_title,
        recruiter,
        hiring_manager,
        requisition_fill_start_date,

        -- Funnel volumes
        total_applicants,
        total_screened,
        total_interviewed,
        total_offered,
        total_hired,
        total_declined,

        -- Stage-to-stage conversion
        safe_divide(total_screened,    total_applicants)    as conversion_applicant_to_screen,
        safe_divide(total_interviewed, total_screened)      as conversion_screen_to_interview,
        safe_divide(total_offered,     total_interviewed)   as conversion_interview_to_offer,
        safe_divide(total_hired,       total_offered)       as conversion_offer_to_hire,

        -- Headline req metrics
        time_to_fill_days,
        safe_divide(total_hired, total_offered)             as offer_acceptance_rate,
        top_source

    from aggregated

)

select * from final