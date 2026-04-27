/*
    Model:        fct_recruiting_velocity
    Layer:        marts
    Sources:      int_recruiting_funnel_metrics
    Materialized: table  (per dbt_project.yml marts defaults)

    Purpose
    -------
    Per-requisition velocity dashboard. Granularity: one row per
    requisition (~592 rows). Carries all funnel + time-to metrics from
    int_recruiting_funnel_metrics, plus department-level peer averages.

    Channel logic (top_application_channel) is computed upstream in
    int_recruiting_funnel_metrics so this mart stays focused on
    department-peer-comparison metrics. The mart's output column names
    (top_source_for_requisition, top_source_count, top_source_share)
    are preserved as the public contract for downstream consumers --
    only the upstream column names changed.

    Calculated fields beyond int_recruiting_funnel_metrics
    ------------------------------------------------------
        hire_year, hire_quarter, hire_year_quarter
            Time bucketing on hire_date for grouping in BI.
        dept_avg_time_to_fill_days, dept_avg_time_to_offer_days
            Department-level peer averages (window).
        time_to_fill_vs_dept_avg, time_to_offer_vs_dept_avg
            Per-requisition deviation from dept average.
        dept_avg_overall_conversion
            Department-level peer average of the Applied -> Hired rate.
*/

{{ config(materialized='table') }}

with funnel as (
    select * from {{ ref('int_recruiting_funnel_metrics') }}
),

with_dept_avgs as (
    select
        f.*,
        avg(f.time_to_fill_days)       over (partition by f.department) as dept_avg_time_to_fill_days,
        avg(f.time_to_offer_days)      over (partition by f.department) as dept_avg_time_to_offer_days,
        avg(f.rate_overall_conversion) over (partition by f.department) as dept_avg_overall_conversion
    from funnel as f
),

final as (
    select
        -- Requisition identity
        requisition_id,
        department,
        sub_department,
        job_title,
        recruiter,
        hiring_manager,

        -- Hire timing buckets
        hired_application_date,
        hire_date,
        extract(year    from hire_date) as hire_year,
        extract(quarter from hire_date) as hire_quarter,
        concat(
            cast(extract(year from hire_date) as string),
            '-Q',
            cast(extract(quarter from hire_date) as string)
        ) as hire_year_quarter,

        -- Funnel volumes
        total_applications,
        reached_phone_screen,
        reached_onsite,
        reached_offer,
        count_hired,
        count_rejected,
        count_withdrawn,

        -- Time-to metrics
        time_to_fill_days,
        time_to_offer_days,
        round(dept_avg_time_to_fill_days,  1) as dept_avg_time_to_fill_days,
        round(dept_avg_time_to_offer_days, 1) as dept_avg_time_to_offer_days,
        round(time_to_fill_days  - dept_avg_time_to_fill_days,  1) as time_to_fill_vs_dept_avg,
        round(time_to_offer_days - dept_avg_time_to_offer_days, 1) as time_to_offer_vs_dept_avg,

        -- Conversion rates
        rate_applied_to_phone,
        rate_phone_to_onsite,
        rate_onsite_to_offer,
        rate_offer_to_hired,
        rate_overall_conversion,
        round(dept_avg_overall_conversion, 4) as dept_avg_overall_conversion,

        -- Channel mix (computed upstream in int_recruiting_funnel_metrics).
        -- Public column names preserved for downstream / Tableau parity;
        -- internal renames keep the bare `source` identifier out of
        -- BigQuery's parser path.
        top_application_channel       as top_source_for_requisition,
        top_application_channel_count as top_source_count,
        top_application_channel_share as top_source_share
    from with_dept_avgs
)

select * from final
