/*
    Model:  int_engagement_theme_rollup
    Layer:  Intermediate
    Source: stg_engagement
    Grain:  One row per (survey_cycle × department × theme)

    Purpose:
        Collapse anonymized individual engagement responses to the
        department × theme grain that fct_engagement_trends ultimately
        publishes. Computes theme-level avg score, response count,
        favorable percentage, eNPS at the (cycle, department) level,
        and cycle-over-cycle deltas.

    Likert vs eNPS:
        stg_engagement rows are mutually exclusive — Likert questions
        carry response_likert (1-5) with eNPS NULL; the eNPS question
        carries enps (0-10) with response_likert NULL. Theme-avg /
        favorable % aggregate the Likert rows; eNPS aggregates the
        eNPS rows separately and is joined back on (cycle, department).
        Every theme row in a given (cycle, dept) carries the same
        eNPS — Tableau can collapse to one eNPS per dept-cycle.

    eNPS formula:
        100 × (% Promoter − % Detractor)
        Promoters = enps_category 'Promoter', Detractors = 'Detractor'.
        Passives drop out of the numerator but stay in the denominator.

    Cycle ordering:
        survey_cycle strings ("2024 H1 Engagement Survey",
        "2024 H2 Engagement Survey") sort chronologically by lexical
        order — year prefix orders years, "H1" < "H2" within a year.
*/

with source as (

    select * from {{ ref('stg_engagement') }}

),

likert_responses as (

    select * from source where response_likert is not null

),

enps_responses as (

    select * from source where enps is not null

),

theme_metrics as (

    select
        ees_cycle           as survey_cycle,
        department,
        ees_theme_name      as theme,
        avg(response_likert)                                            as theme_avg_score,
        count(*)                                                        as response_count,
        safe_divide(countif(response_likert >= 4), count(*))            as favorable_pct
    from likert_responses
    group by survey_cycle, department, theme

),

enps_per_dept_cycle as (

    select
        ees_cycle           as survey_cycle,
        department,
        100 * safe_divide(
            countif(enps_category = 'Promoter') - countif(enps_category = 'Detractor'),
            count(*)
        ) as enps_score
    from enps_responses
    group by survey_cycle, department

),

joined as (

    select
        t.survey_cycle,
        t.department,
        t.theme,
        t.theme_avg_score,
        t.response_count,
        t.favorable_pct,
        e.enps_score
    from theme_metrics as t
    left join enps_per_dept_cycle as e
        on  t.survey_cycle = e.survey_cycle
        and t.department   = e.department

),

final as (

    select
        survey_cycle,
        department,
        theme,
        theme_avg_score,
        response_count,
        favorable_pct,
        enps_score,
        lag(theme_avg_score) over cycle_window      as prior_cycle_theme_avg_score,
        theme_avg_score
            - lag(theme_avg_score) over cycle_window as theme_avg_score_delta,
        lag(favorable_pct) over cycle_window         as prior_cycle_favorable_pct,
        favorable_pct
            - lag(favorable_pct) over cycle_window   as favorable_pct_delta
    from joined
    window cycle_window as (
        partition by department, theme
        order by survey_cycle
    )

)

select * from final
