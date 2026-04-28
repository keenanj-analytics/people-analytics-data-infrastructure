/*
    Model:        fct_monthly_metrics
    Layer:        marts
    Sources:      int_monthly_headcount_snapshot
                  int_employee_dimension
                  int_recruiting_funnel_metrics
                  fct_attrition_drivers
                  fct_engagement_trends
                  stg_compensation
                  stg_performance
    Materialized: table

    Purpose
    -------
    Centralized monthly metrics mart -- one wide row per
    (snapshot_month, segment_type, segment_value) carrying base
    monthly values plus rolling-window aggregations (quarterly,
    rolling_6mo, ttm) for each metric. Single source of truth for
    Tableau; consumers never compute their own rolling aggregates.

    Granularity
    -----------
    One row per (snapshot_month, segment_type, segment_value).
    Six segment_types: overall, department, sub_department,
    job_level, location_state, tenure_band. ~5,000 rows total
    across 63 months.

    Window aggregations
    -------------------
    Each metric exposed at four levels:
        _monthly       this month's base value
        _quarterly     ROWS BETWEEN  2 PRECEDING AND CURRENT
        _rolling_6mo   ROWS BETWEEN  5 PRECEDING AND CURRENT
        _ttm           ROWS BETWEEN 11 PRECEDING AND CURRENT
    Aggregation function:
        active_headcount, avg_compa_ratio, avg_performance_rating,
        avg_time_to_fill_days, offer_acceptance_rate, avg_enps,
        theme_*_score                                        -> AVG
        hires, terminations_*                                -> SUM

    Annualized attrition rates
    --------------------------
    attrition_rate_<type>_<window> =
        sum(terminations over window) /
        avg(active_headcount over window) * annualization_factor
    where annualization is 12 / 4 / 2 / 1 for monthly / quarterly /
    rolling_6mo / ttm respectively. Early months in the spine
    annualize over partial windows (ttm in 2020-Q1 reflects only
    available history, not a full year).

    Engagement coverage
    -------------------
    avg_enps and the eight theme scores (theme_*_score) come from
    fct_engagement_trends and only populate for segment_type =
    'department'. Engagement is anonymized at the dept level, so
    finer segments stay NULL. Each calendar month within a survey
    quarter carries that quarter's value (Jan/Feb/Mar 2024 all show
    2024-Q1 engagement).

    Carry-forward semantics
    -----------------------
    Segment dimensions inherit from int_monthly_headcount_snapshot's
    carry-forward, so a 2022 hire's hire-month row uses their
    CURRENT segment, not their at-hire segment. See snapshot model
    docstring for the trade-off. Same convention for terminations.

    Recruiting metrics scope
    ------------------------
    avg_time_to_fill_days and offer_acceptance_rate populate only
    for segment_types 'overall', 'department', and 'sub_department'
    -- requisitions don't carry job_level / location_state /
    tenure_band dimensions, so other segments remain NULL.
*/

{{ config(materialized='table') }}

with month_spine as (
    select snapshot_month
    from unnest(
        generate_date_array(date '2020-01-01', date '2025-03-01', interval 1 month)
    ) as snapshot_month
),

employees as (
    select
        employee_id,
        department,
        sub_department,
        job_level,
        location_state,
        tenure_band,
        hire_date,
        termination_date,
        termination_type
    from {{ ref('int_employee_dimension') }}
),

attrition_top_performer as (
    select
        employee_id,
        is_top_performer
    from {{ ref('fct_attrition_drivers') }}
),

headcount as (
    select * from {{ ref('int_monthly_headcount_snapshot') }}
),

-- ============================================================
-- Point-in-time compa_ratio at each (employee, snapshot_month)
-- ============================================================
comp_at_snapshot as (
    select
        h.employee_id,
        h.snapshot_month,
        safe_divide(c.salary, c.comp_band_mid) as compa_ratio
    from headcount as h
    left join {{ ref('stg_compensation') }} as c
      on c.employee_id = h.employee_id
     and c.effective_date <= last_day(h.snapshot_month, month)
    qualify row_number() over (
        partition by h.employee_id, h.snapshot_month
        order by c.effective_date desc
    ) = 1
),

-- ============================================================
-- Point-in-time performance rating (latest review whose
-- review_completed_date <= snapshot month-end)
-- ============================================================
perf_at_snapshot as (
    select
        h.employee_id,
        h.snapshot_month,
        case p.overall_rating
            when 'Significantly Exceeds' then 5
            when 'Exceeds'               then 4
            when 'Meets'                 then 3
            when 'Partially Meets'       then 2
            when 'Does Not Meet'         then 1
        end as rating_numeric
    from headcount as h
    left join {{ ref('stg_performance') }} as p
      on p.employee_id = h.employee_id
     and p.review_completed_date <= last_day(h.snapshot_month, month)
    qualify row_number() over (
        partition by h.employee_id, h.snapshot_month
        order by p.review_completed_date desc nulls last
    ) = 1
),

-- ============================================================
-- One row per (employee, month) with all attrs needed below
-- ============================================================
employee_month as (
    select
        h.snapshot_month,
        h.employee_id,
        h.department,
        h.sub_department,
        h.job_level,
        h.location_state,
        h.tenure_band,
        c.compa_ratio,
        p.rating_numeric
    from headcount as h
    left join comp_at_snapshot as c
      on  c.employee_id    = h.employee_id
      and c.snapshot_month = h.snapshot_month
    left join perf_at_snapshot as p
      on  p.employee_id    = h.employee_id
      and p.snapshot_month = h.snapshot_month
),

-- ============================================================
-- Long-format segment expansion: each (employee, month) -> 6 rows
-- ============================================================
employee_segments as (
    select snapshot_month, 'overall'        as segment_type, 'All'              as segment_value, compa_ratio, rating_numeric from employee_month
    union all
    select snapshot_month, 'department',     department,     compa_ratio, rating_numeric from employee_month
    union all
    select snapshot_month, 'sub_department', sub_department, compa_ratio, rating_numeric from employee_month where sub_department is not null
    union all
    select snapshot_month, 'job_level',      job_level,      compa_ratio, rating_numeric from employee_month
    union all
    select snapshot_month, 'location_state', location_state, compa_ratio, rating_numeric from employee_month
    union all
    select snapshot_month, 'tenure_band',    tenure_band,    compa_ratio, rating_numeric from employee_month
),

base_per_segment as (
    select
        snapshot_month,
        segment_type,
        segment_value,
        count(*)            as active_headcount_monthly,
        avg(compa_ratio)    as avg_compa_ratio_monthly_raw,
        avg(rating_numeric) as avg_performance_rating_monthly_raw
    from employee_segments
    group by snapshot_month, segment_type, segment_value
),

-- ============================================================
-- Hires per (hire-month, segment)
-- ============================================================
hires_long as (
    select
        date_trunc(hire_date, month) as snapshot_month,
        department, sub_department, job_level, location_state, tenure_band
    from employees
    where hire_date between date '2020-01-01' and date '2025-03-31'
),

hires_segments as (
    select snapshot_month, 'overall'        as segment_type, 'All'              as segment_value from hires_long
    union all
    select snapshot_month, 'department',     department     from hires_long
    union all
    select snapshot_month, 'sub_department', sub_department from hires_long where sub_department is not null
    union all
    select snapshot_month, 'job_level',      job_level      from hires_long
    union all
    select snapshot_month, 'location_state', location_state from hires_long
    union all
    select snapshot_month, 'tenure_band',    tenure_band    from hires_long
),

hires_per_segment as (
    select
        snapshot_month, segment_type, segment_value,
        count(*) as hires_monthly
    from hires_segments
    group by snapshot_month, segment_type, segment_value
),

-- ============================================================
-- Terminations per (term-month, segment), with type + top-perf
-- ============================================================
terminations_long as (
    select
        date_trunc(e.termination_date, month) as snapshot_month,
        e.department, e.sub_department, e.job_level, e.location_state, e.tenure_band,
        e.termination_type,
        coalesce(a.is_top_performer, false) as is_top_performer
    from employees as e
    left join attrition_top_performer as a using (employee_id)
    where e.termination_date between date '2020-01-01' and date '2025-03-31'
),

terminations_segments as (
    select snapshot_month, 'overall'        as segment_type, 'All'              as segment_value, termination_type, is_top_performer from terminations_long
    union all
    select snapshot_month, 'department',     department,     termination_type, is_top_performer from terminations_long
    union all
    select snapshot_month, 'sub_department', sub_department, termination_type, is_top_performer from terminations_long where sub_department is not null
    union all
    select snapshot_month, 'job_level',      job_level,      termination_type, is_top_performer from terminations_long
    union all
    select snapshot_month, 'location_state', location_state, termination_type, is_top_performer from terminations_long
    union all
    select snapshot_month, 'tenure_band',    tenure_band,    termination_type, is_top_performer from terminations_long
),

terminations_per_segment as (
    select
        snapshot_month, segment_type, segment_value,
        count(*)                                as terminations_overall_monthly,
        countif(termination_type = 'Voluntary') as terminations_voluntary_monthly,
        countif(is_top_performer)               as terminations_top_performer_monthly
    from terminations_segments
    group by snapshot_month, segment_type, segment_value
),

-- ============================================================
-- Recruiting metrics per (hire-month, segment).
-- Only overall / department / sub_department populate.
-- ============================================================
recruiting as (
    select
        date_trunc(hire_date, month) as snapshot_month,
        department,
        sub_department,
        time_to_fill_days,
        offer_acceptance_rate
    from {{ ref('int_recruiting_funnel_metrics') }}
    where hire_date is not null
),

recruiting_segments as (
    select snapshot_month, 'overall'        as segment_type, 'All'              as segment_value, time_to_fill_days, offer_acceptance_rate from recruiting
    union all
    select snapshot_month, 'department',     department,     time_to_fill_days, offer_acceptance_rate from recruiting
    union all
    select snapshot_month, 'sub_department', sub_department, time_to_fill_days, offer_acceptance_rate from recruiting where sub_department is not null
),

recruiting_per_segment as (
    select
        snapshot_month, segment_type, segment_value,
        avg(time_to_fill_days)     as avg_time_to_fill_days_monthly_raw,
        avg(offer_acceptance_rate) as offer_acceptance_rate_monthly_raw
    from recruiting_segments
    group by snapshot_month, segment_type, segment_value
),

-- ============================================================
-- Engagement: pivot 8 themes -> 8 columns per (cycle, dept).
-- Map each calendar month to its containing survey quarter.
-- ============================================================
engagement_pivot as (
    select
        extract(year    from cycle_end_date) as cycle_year,
        extract(quarter from cycle_end_date) as cycle_quarter,
        department,
        any_value(enps_score) as avg_enps_monthly,
        max(case when theme = 'Employee Engagement'         then theme_avg_score end) as theme_employee_engagement_score_monthly,
        max(case when theme = 'Manager Effectiveness'       then theme_avg_score end) as theme_manager_effectiveness_score_monthly,
        max(case when theme = 'Career Growth & Development' then theme_avg_score end) as theme_career_growth_score_monthly,
        max(case when theme = 'Work-Life Balance'           then theme_avg_score end) as theme_work_life_balance_score_monthly,
        max(case when theme = 'Recognition'                 then theme_avg_score end) as theme_recognition_score_monthly,
        max(case when theme = 'Company Culture'             then theme_avg_score end) as theme_company_culture_score_monthly,
        max(case when theme = 'Communication'               then theme_avg_score end) as theme_communication_score_monthly,
        max(case when theme = 'Resources & Enablement'      then theme_avg_score end) as theme_enablement_score_monthly
    from {{ ref('fct_engagement_trends') }}
    group by cycle_year, cycle_quarter, department
),

engagement_per_month as (
    select
        m.snapshot_month,
        'department'   as segment_type,
        e.department   as segment_value,
        e.avg_enps_monthly,
        e.theme_employee_engagement_score_monthly,
        e.theme_manager_effectiveness_score_monthly,
        e.theme_career_growth_score_monthly,
        e.theme_work_life_balance_score_monthly,
        e.theme_recognition_score_monthly,
        e.theme_company_culture_score_monthly,
        e.theme_communication_score_monthly,
        e.theme_enablement_score_monthly
    from month_spine as m
    inner join engagement_pivot as e
      on  e.cycle_year    = extract(year    from m.snapshot_month)
      and e.cycle_quarter = extract(quarter from m.snapshot_month)
),

-- ============================================================
-- Combine all metrics on the base spine
-- ============================================================
combined as (
    select
        b.snapshot_month,
        b.segment_type,
        b.segment_value,

        -- Base
        b.active_headcount_monthly,
        b.avg_compa_ratio_monthly_raw,
        b.avg_performance_rating_monthly_raw,

        -- Events (zero-fill so SUM windows aren't poisoned by NULLs)
        coalesce(h.hires_monthly,                       0) as hires_monthly,
        coalesce(t.terminations_overall_monthly,        0) as terminations_overall_monthly,
        coalesce(t.terminations_voluntary_monthly,      0) as terminations_voluntary_monthly,
        coalesce(t.terminations_top_performer_monthly,  0) as terminations_top_performer_monthly,

        -- Recruiting (NULL where no requisitions filled)
        r.avg_time_to_fill_days_monthly_raw,
        r.offer_acceptance_rate_monthly_raw,

        -- Engagement (NULL except segment_type = 'department')
        e.avg_enps_monthly,
        e.theme_employee_engagement_score_monthly,
        e.theme_manager_effectiveness_score_monthly,
        e.theme_career_growth_score_monthly,
        e.theme_work_life_balance_score_monthly,
        e.theme_recognition_score_monthly,
        e.theme_company_culture_score_monthly,
        e.theme_communication_score_monthly,
        e.theme_enablement_score_monthly
    from base_per_segment as b
    left join hires_per_segment as h
      on  h.snapshot_month = b.snapshot_month
      and h.segment_type   = b.segment_type
      and h.segment_value  = b.segment_value
    left join terminations_per_segment as t
      on  t.snapshot_month = b.snapshot_month
      and t.segment_type   = b.segment_type
      and t.segment_value  = b.segment_value
    left join recruiting_per_segment as r
      on  r.snapshot_month = b.snapshot_month
      and r.segment_type   = b.segment_type
      and r.segment_value  = b.segment_value
    left join engagement_per_month as e
      on  e.snapshot_month = b.snapshot_month
      and e.segment_type   = b.segment_type
      and e.segment_value  = b.segment_value
),

-- ============================================================
-- Rolling window aggregations
-- ============================================================
windowed as (
    select
        snapshot_month,
        segment_type,
        segment_value,

        active_headcount_monthly,
        avg(active_headcount_monthly) over w_3   as active_headcount_quarterly,
        avg(active_headcount_monthly) over w_6   as active_headcount_rolling_6mo,
        avg(active_headcount_monthly) over w_12  as active_headcount_ttm,

        hires_monthly,
        sum(hires_monthly) over w_3   as hires_quarterly,
        sum(hires_monthly) over w_6   as hires_rolling_6mo,
        sum(hires_monthly) over w_12  as hires_ttm,

        terminations_overall_monthly,
        sum(terminations_overall_monthly) over w_3   as terminations_overall_quarterly,
        sum(terminations_overall_monthly) over w_6   as terminations_overall_rolling_6mo,
        sum(terminations_overall_monthly) over w_12  as terminations_overall_ttm,

        terminations_voluntary_monthly,
        sum(terminations_voluntary_monthly) over w_3   as terminations_voluntary_quarterly,
        sum(terminations_voluntary_monthly) over w_6   as terminations_voluntary_rolling_6mo,
        sum(terminations_voluntary_monthly) over w_12  as terminations_voluntary_ttm,

        terminations_top_performer_monthly,
        sum(terminations_top_performer_monthly) over w_3   as terminations_top_performer_quarterly,
        sum(terminations_top_performer_monthly) over w_6   as terminations_top_performer_rolling_6mo,
        sum(terminations_top_performer_monthly) over w_12  as terminations_top_performer_ttm,

        avg_time_to_fill_days_monthly_raw,
        avg(avg_time_to_fill_days_monthly_raw) over w_3   as avg_time_to_fill_days_quarterly_raw,
        avg(avg_time_to_fill_days_monthly_raw) over w_6   as avg_time_to_fill_days_rolling_6mo_raw,
        avg(avg_time_to_fill_days_monthly_raw) over w_12  as avg_time_to_fill_days_ttm_raw,

        offer_acceptance_rate_monthly_raw,
        avg(offer_acceptance_rate_monthly_raw) over w_3   as offer_acceptance_rate_quarterly_raw,
        avg(offer_acceptance_rate_monthly_raw) over w_6   as offer_acceptance_rate_rolling_6mo_raw,
        avg(offer_acceptance_rate_monthly_raw) over w_12  as offer_acceptance_rate_ttm_raw,

        avg_compa_ratio_monthly_raw,
        avg(avg_compa_ratio_monthly_raw) over w_3   as avg_compa_ratio_quarterly_raw,
        avg(avg_compa_ratio_monthly_raw) over w_6   as avg_compa_ratio_rolling_6mo_raw,
        avg(avg_compa_ratio_monthly_raw) over w_12  as avg_compa_ratio_ttm_raw,

        avg_performance_rating_monthly_raw,
        avg(avg_performance_rating_monthly_raw) over w_3   as avg_performance_rating_quarterly_raw,
        avg(avg_performance_rating_monthly_raw) over w_6   as avg_performance_rating_rolling_6mo_raw,
        avg(avg_performance_rating_monthly_raw) over w_12  as avg_performance_rating_ttm_raw,

        avg_enps_monthly,
        avg(avg_enps_monthly) over w_3   as avg_enps_quarterly_raw,
        avg(avg_enps_monthly) over w_6   as avg_enps_rolling_6mo_raw,
        avg(avg_enps_monthly) over w_12  as avg_enps_ttm_raw,

        theme_employee_engagement_score_monthly,
        avg(theme_employee_engagement_score_monthly) over w_3   as theme_employee_engagement_score_quarterly_raw,
        avg(theme_employee_engagement_score_monthly) over w_6   as theme_employee_engagement_score_rolling_6mo_raw,
        avg(theme_employee_engagement_score_monthly) over w_12  as theme_employee_engagement_score_ttm_raw,

        theme_manager_effectiveness_score_monthly,
        avg(theme_manager_effectiveness_score_monthly) over w_3   as theme_manager_effectiveness_score_quarterly_raw,
        avg(theme_manager_effectiveness_score_monthly) over w_6   as theme_manager_effectiveness_score_rolling_6mo_raw,
        avg(theme_manager_effectiveness_score_monthly) over w_12  as theme_manager_effectiveness_score_ttm_raw,

        theme_career_growth_score_monthly,
        avg(theme_career_growth_score_monthly) over w_3   as theme_career_growth_score_quarterly_raw,
        avg(theme_career_growth_score_monthly) over w_6   as theme_career_growth_score_rolling_6mo_raw,
        avg(theme_career_growth_score_monthly) over w_12  as theme_career_growth_score_ttm_raw,

        theme_work_life_balance_score_monthly,
        avg(theme_work_life_balance_score_monthly) over w_3   as theme_work_life_balance_score_quarterly_raw,
        avg(theme_work_life_balance_score_monthly) over w_6   as theme_work_life_balance_score_rolling_6mo_raw,
        avg(theme_work_life_balance_score_monthly) over w_12  as theme_work_life_balance_score_ttm_raw,

        theme_recognition_score_monthly,
        avg(theme_recognition_score_monthly) over w_3   as theme_recognition_score_quarterly_raw,
        avg(theme_recognition_score_monthly) over w_6   as theme_recognition_score_rolling_6mo_raw,
        avg(theme_recognition_score_monthly) over w_12  as theme_recognition_score_ttm_raw,

        theme_company_culture_score_monthly,
        avg(theme_company_culture_score_monthly) over w_3   as theme_company_culture_score_quarterly_raw,
        avg(theme_company_culture_score_monthly) over w_6   as theme_company_culture_score_rolling_6mo_raw,
        avg(theme_company_culture_score_monthly) over w_12  as theme_company_culture_score_ttm_raw,

        theme_communication_score_monthly,
        avg(theme_communication_score_monthly) over w_3   as theme_communication_score_quarterly_raw,
        avg(theme_communication_score_monthly) over w_6   as theme_communication_score_rolling_6mo_raw,
        avg(theme_communication_score_monthly) over w_12  as theme_communication_score_ttm_raw,

        theme_enablement_score_monthly,
        avg(theme_enablement_score_monthly) over w_3   as theme_enablement_score_quarterly_raw,
        avg(theme_enablement_score_monthly) over w_6   as theme_enablement_score_rolling_6mo_raw,
        avg(theme_enablement_score_monthly) over w_12  as theme_enablement_score_ttm_raw
    from combined
    window
        w_3  as (partition by segment_type, segment_value order by snapshot_month rows between  2 preceding and current row),
        w_6  as (partition by segment_type, segment_value order by snapshot_month rows between  5 preceding and current row),
        w_12 as (partition by segment_type, segment_value order by snapshot_month rows between 11 preceding and current row)
),

final as (
    select
        snapshot_month,
        segment_type,
        segment_value,

        -- Active headcount
        active_headcount_monthly,
        round(active_headcount_quarterly,    2) as active_headcount_quarterly,
        round(active_headcount_rolling_6mo,  2) as active_headcount_rolling_6mo,
        round(active_headcount_ttm,          2) as active_headcount_ttm,

        -- Hires
        hires_monthly,
        hires_quarterly,
        hires_rolling_6mo,
        hires_ttm,

        -- Terminations
        terminations_overall_monthly,
        terminations_overall_quarterly,
        terminations_overall_rolling_6mo,
        terminations_overall_ttm,

        terminations_voluntary_monthly,
        terminations_voluntary_quarterly,
        terminations_voluntary_rolling_6mo,
        terminations_voluntary_ttm,

        terminations_top_performer_monthly,
        terminations_top_performer_quarterly,
        terminations_top_performer_rolling_6mo,
        terminations_top_performer_ttm,

        -- Annualized attrition rates
        round(safe_divide(terminations_overall_monthly,         active_headcount_monthly      ) * 12, 4) as attrition_rate_overall_monthly,
        round(safe_divide(terminations_overall_quarterly,       active_headcount_quarterly    ) *  4, 4) as attrition_rate_overall_quarterly,
        round(safe_divide(terminations_overall_rolling_6mo,     active_headcount_rolling_6mo  ) *  2, 4) as attrition_rate_overall_rolling_6mo,
        round(safe_divide(terminations_overall_ttm,             active_headcount_ttm          ) *  1, 4) as attrition_rate_overall_ttm,

        round(safe_divide(terminations_voluntary_monthly,       active_headcount_monthly      ) * 12, 4) as attrition_rate_voluntary_monthly,
        round(safe_divide(terminations_voluntary_quarterly,     active_headcount_quarterly    ) *  4, 4) as attrition_rate_voluntary_quarterly,
        round(safe_divide(terminations_voluntary_rolling_6mo,   active_headcount_rolling_6mo  ) *  2, 4) as attrition_rate_voluntary_rolling_6mo,
        round(safe_divide(terminations_voluntary_ttm,           active_headcount_ttm          ) *  1, 4) as attrition_rate_voluntary_ttm,

        round(safe_divide(terminations_top_performer_monthly,    active_headcount_monthly     ) * 12, 4) as attrition_rate_top_performer_monthly,
        round(safe_divide(terminations_top_performer_quarterly,  active_headcount_quarterly   ) *  4, 4) as attrition_rate_top_performer_quarterly,
        round(safe_divide(terminations_top_performer_rolling_6mo,active_headcount_rolling_6mo ) *  2, 4) as attrition_rate_top_performer_rolling_6mo,
        round(safe_divide(terminations_top_performer_ttm,        active_headcount_ttm         ) *  1, 4) as attrition_rate_top_performer_ttm,

        -- Recruiting
        round(avg_time_to_fill_days_monthly_raw,        1) as avg_time_to_fill_days_monthly,
        round(avg_time_to_fill_days_quarterly_raw,      1) as avg_time_to_fill_days_quarterly,
        round(avg_time_to_fill_days_rolling_6mo_raw,    1) as avg_time_to_fill_days_rolling_6mo,
        round(avg_time_to_fill_days_ttm_raw,            1) as avg_time_to_fill_days_ttm,

        round(offer_acceptance_rate_monthly_raw,        4) as offer_acceptance_rate_monthly,
        round(offer_acceptance_rate_quarterly_raw,      4) as offer_acceptance_rate_quarterly,
        round(offer_acceptance_rate_rolling_6mo_raw,    4) as offer_acceptance_rate_rolling_6mo,
        round(offer_acceptance_rate_ttm_raw,            4) as offer_acceptance_rate_ttm,

        -- Compa ratio
        round(avg_compa_ratio_monthly_raw,              4) as avg_compa_ratio_monthly,
        round(avg_compa_ratio_quarterly_raw,            4) as avg_compa_ratio_quarterly,
        round(avg_compa_ratio_rolling_6mo_raw,          4) as avg_compa_ratio_rolling_6mo,
        round(avg_compa_ratio_ttm_raw,                  4) as avg_compa_ratio_ttm,

        -- Performance rating
        round(avg_performance_rating_monthly_raw,       2) as avg_performance_rating_monthly,
        round(avg_performance_rating_quarterly_raw,     2) as avg_performance_rating_quarterly,
        round(avg_performance_rating_rolling_6mo_raw,   2) as avg_performance_rating_rolling_6mo,
        round(avg_performance_rating_ttm_raw,           2) as avg_performance_rating_ttm,

        -- Engagement: eNPS
        avg_enps_monthly,
        round(avg_enps_quarterly_raw,    1) as avg_enps_quarterly,
        round(avg_enps_rolling_6mo_raw,  1) as avg_enps_rolling_6mo,
        round(avg_enps_ttm_raw,          1) as avg_enps_ttm,

        -- Engagement: theme scores (8 themes x 4 windows)
        theme_employee_engagement_score_monthly,
        round(theme_employee_engagement_score_quarterly_raw,    2) as theme_employee_engagement_score_quarterly,
        round(theme_employee_engagement_score_rolling_6mo_raw,  2) as theme_employee_engagement_score_rolling_6mo,
        round(theme_employee_engagement_score_ttm_raw,          2) as theme_employee_engagement_score_ttm,

        theme_manager_effectiveness_score_monthly,
        round(theme_manager_effectiveness_score_quarterly_raw,    2) as theme_manager_effectiveness_score_quarterly,
        round(theme_manager_effectiveness_score_rolling_6mo_raw,  2) as theme_manager_effectiveness_score_rolling_6mo,
        round(theme_manager_effectiveness_score_ttm_raw,          2) as theme_manager_effectiveness_score_ttm,

        theme_career_growth_score_monthly,
        round(theme_career_growth_score_quarterly_raw,    2) as theme_career_growth_score_quarterly,
        round(theme_career_growth_score_rolling_6mo_raw,  2) as theme_career_growth_score_rolling_6mo,
        round(theme_career_growth_score_ttm_raw,          2) as theme_career_growth_score_ttm,

        theme_work_life_balance_score_monthly,
        round(theme_work_life_balance_score_quarterly_raw,    2) as theme_work_life_balance_score_quarterly,
        round(theme_work_life_balance_score_rolling_6mo_raw,  2) as theme_work_life_balance_score_rolling_6mo,
        round(theme_work_life_balance_score_ttm_raw,          2) as theme_work_life_balance_score_ttm,

        theme_recognition_score_monthly,
        round(theme_recognition_score_quarterly_raw,    2) as theme_recognition_score_quarterly,
        round(theme_recognition_score_rolling_6mo_raw,  2) as theme_recognition_score_rolling_6mo,
        round(theme_recognition_score_ttm_raw,          2) as theme_recognition_score_ttm,

        theme_company_culture_score_monthly,
        round(theme_company_culture_score_quarterly_raw,    2) as theme_company_culture_score_quarterly,
        round(theme_company_culture_score_rolling_6mo_raw,  2) as theme_company_culture_score_rolling_6mo,
        round(theme_company_culture_score_ttm_raw,          2) as theme_company_culture_score_ttm,

        theme_communication_score_monthly,
        round(theme_communication_score_quarterly_raw,    2) as theme_communication_score_quarterly,
        round(theme_communication_score_rolling_6mo_raw,  2) as theme_communication_score_rolling_6mo,
        round(theme_communication_score_ttm_raw,          2) as theme_communication_score_ttm,

        theme_enablement_score_monthly,
        round(theme_enablement_score_quarterly_raw,    2) as theme_enablement_score_quarterly,
        round(theme_enablement_score_rolling_6mo_raw,  2) as theme_enablement_score_rolling_6mo,
        round(theme_enablement_score_ttm_raw,          2) as theme_enablement_score_ttm
    from windowed
)

select * from final
order by segment_type, segment_value, snapshot_month
