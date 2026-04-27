/*
    Model:        int_engagement_theme_rollup
    Layer:        intermediate
    Sources:      stg_engagement
    Materialized: view  (per dbt_project.yml intermediate defaults)

    Purpose
    -------
    Roll the 27 standard engagement questions up to the 8 theme level
    so dashboards can chart "Manager Effectiveness over time" without
    aggregating five MGE rows per cycle in the BI tool. One row per
    (survey_cycle, department, sub_department, theme).

    Calculated fields
    -----------------
        theme_avg_score              mean of avg_score across questions in the theme
        theme_avg_favorable_pct      mean of favorable_pct across questions in the theme
        questions_in_theme           count of questions rolled into this theme row
        cycle_end_date               parsed from survey_cycle (Q1 -> Mar 31, etc.)
        prior_theme_avg_score        lag for cycle-over-cycle trend
        theme_score_delta            theme_avg_score - prior_theme_avg_score
        prior_theme_avg_favorable_pct, theme_favorable_pct_delta
                                     same trend pattern for the favorable percentage

    response_count and enps_score are denormalized per (cycle, dept) in
    stg_engagement -- the same values across every question -- so we
    pass them through with any_value() rather than aggregating.

    Granularity caveat
    ------------------
    sub_department is currently null on every input row (raw_engagement
    emits dept-level only in this build). The compound key here is
    therefore (survey_cycle, department, theme) in practice, with
    sub_department reserved for forward compatibility when the
    generator emits sub-dept rows for sub-depts with 5+ respondents.
*/



with engagement as (
    select * from `just-kaizen-ai`.`raw_staging`.`stg_engagement`
),

per_theme as (
    select
        survey_cycle,
        department,
        sub_department,
        theme,

        round(avg(avg_score),     2) as theme_avg_score,
        round(avg(favorable_pct), 4) as theme_avg_favorable_pct,
        count(*)                     as questions_in_theme,

        -- Denormalized in source -- same value across all question rows
        any_value(response_count) as response_count,
        any_value(enps_score)     as enps_score
    from engagement
    group by survey_cycle, department, sub_department, theme
),

with_cycle_date as (
    select
        *,
        case substr(survey_cycle, 6, 2)
            when 'Q1' then date(cast(left(survey_cycle, 4) as int64),  3, 31)
            when 'Q2' then date(cast(left(survey_cycle, 4) as int64),  6, 30)
            when 'Q3' then date(cast(left(survey_cycle, 4) as int64),  9, 30)
            when 'Q4' then date(cast(left(survey_cycle, 4) as int64), 12, 31)
        end as cycle_end_date
    from per_theme
),

with_trends as (
    select
        *,
        lag(theme_avg_score) over (
            partition by department, sub_department, theme
            order by cycle_end_date
        ) as prior_theme_avg_score,
        lag(theme_avg_favorable_pct) over (
            partition by department, sub_department, theme
            order by cycle_end_date
        ) as prior_theme_avg_favorable_pct
    from with_cycle_date
),

final as (
    select
        survey_cycle,
        cycle_end_date,
        department,
        sub_department,
        theme,
        questions_in_theme,
        response_count,
        theme_avg_score,
        theme_avg_favorable_pct,
        enps_score,
        prior_theme_avg_score,
        prior_theme_avg_favorable_pct,
        case
            when prior_theme_avg_score is not null
                then round(theme_avg_score - prior_theme_avg_score, 2)
            else null
        end as theme_score_delta,
        case
            when prior_theme_avg_favorable_pct is not null
                then round(theme_avg_favorable_pct - prior_theme_avg_favorable_pct, 4)
            else null
        end as theme_favorable_pct_delta
    from with_trends
)

select * from final