/*
    Model:        fct_engagement_trends
    Layer:        Mart
    Materialized: table
    Grain:        One row per (survey_cycle × department × theme)
    Source:       int_engagement_theme_rollup, stg_engagement

    Purpose:
        Theme-level engagement trends for Tableau. Anonymized data --
        no employee_id, so this mart cannot drill through to the
        individual roster. Department-level cuts only.

        Enriches the department-level rollup with org-wide benchmarks
        computed from raw responses (weighted by actual respondent count,
        not averaged across departments). Every row carries:
          - Department-level metrics (from intermediate rollup)
          - Org-wide theme_avg_score and favorable_pct (per cycle × theme)
          - Org-wide eNPS (per cycle)
          - Cycle-over-cycle deltas for all org-wide metrics
*/

with rollup_source as (

    select * from {{ ref('int_engagement_theme_rollup') }}

),

-- ---------------------------------------------------------------
-- Org-wide theme metrics: weighted by actual response count
-- ---------------------------------------------------------------

orgwide_likert as (

    select * from {{ ref('stg_engagement') }}
    where response_likert is not null

),

orgwide_theme_metrics as (

    select
        ees_cycle                                                       as survey_cycle,
        ees_theme_name                                                  as theme,
        avg(response_likert)                                            as orgwide_theme_avg_score,
        count(*)                                                        as orgwide_response_count,
        safe_divide(countif(response_likert >= 4), count(*))            as orgwide_favorable_pct
    from orgwide_likert
    group by survey_cycle, theme

),

orgwide_theme_with_deltas as (

    select
        survey_cycle,
        theme,
        orgwide_theme_avg_score,
        orgwide_response_count,
        orgwide_favorable_pct,
        orgwide_theme_avg_score
            - lag(orgwide_theme_avg_score) over (
                partition by theme order by survey_cycle
            ) as orgwide_theme_avg_score_delta,
        orgwide_favorable_pct
            - lag(orgwide_favorable_pct) over (
                partition by theme order by survey_cycle
            ) as orgwide_favorable_pct_delta
    from orgwide_theme_metrics

),

-- ---------------------------------------------------------------
-- Org-wide eNPS: true calculation from raw responses
-- ---------------------------------------------------------------

orgwide_enps_responses as (

    select * from {{ ref('stg_engagement') }}
    where enps is not null

),

orgwide_enps as (

    select
        ees_cycle                                                       as survey_cycle,
        100 * safe_divide(
            countif(enps_category = 'Promoter') - countif(enps_category = 'Detractor'),
            count(*)
        ) as orgwide_enps_score
    from orgwide_enps_responses
    group by survey_cycle

),

orgwide_enps_with_deltas as (

    select
        survey_cycle,
        orgwide_enps_score,
        orgwide_enps_score
            - lag(orgwide_enps_score) over (
                order by survey_cycle
            ) as orgwide_enps_score_delta
    from orgwide_enps

),

-- ---------------------------------------------------------------
-- Join org-wide benchmarks to department-level rollup
-- ---------------------------------------------------------------

final as (

    select
        r.survey_cycle,
        r.department,
        r.theme,

        -- Department-level metrics
        r.theme_avg_score,
        r.response_count,
        r.favorable_pct,
        r.enps_score,
        r.prior_cycle_theme_avg_score,
        r.theme_avg_score_delta,
        r.prior_cycle_favorable_pct,
        r.favorable_pct_delta,

        -- Org-wide theme benchmarks (per cycle × theme)
        ot.orgwide_theme_avg_score,
        ot.orgwide_response_count,
        ot.orgwide_favorable_pct,
        ot.orgwide_theme_avg_score_delta,
        ot.orgwide_favorable_pct_delta,

        -- Org-wide eNPS benchmarks (per cycle)
        oe.orgwide_enps_score,
        oe.orgwide_enps_score_delta

    from rollup_source as r
    left join orgwide_theme_with_deltas as ot
        on  r.survey_cycle = ot.survey_cycle
        and r.theme        = ot.theme
    left join orgwide_enps_with_deltas as oe
        on  r.survey_cycle = oe.survey_cycle

)

select * from final
