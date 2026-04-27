
  
    

    create or replace table `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`
      
    
    

    
    OPTIONS(
      description="""Per-requisition velocity dashboard with funnel volumes,\ntime-to-fill / time-to-offer, conversion rates, department peer\naverages, and the dominant source channel for each requisition.\n"""
    )
    as (
      /*
    Model:        fct_recruiting_velocity
    Layer:        marts
    Sources:      int_recruiting_funnel_metrics, stg_recruiting
    Materialized: table  (per dbt_project.yml marts defaults)

    Purpose
    -------
    Per-requisition velocity dashboard. Granularity: one row per
    requisition (~592 rows). Carries all funnel + time-to metrics from
    int_recruiting_funnel_metrics, plus department-level peer averages
    and the dominant source channel for each requisition's applicants.

    Calculated fields beyond int_recruiting_funnel_metrics
    ------------------------------------------------------
        hire_year, hire_quarter, hire_year_quarter
            Time bucketing on hire_date for grouping in BI.
        dept_avg_time_to_fill_days, dept_avg_time_to_offer_days
            Department-level peer averages (window).
        time_to_fill_vs_dept_avg
            Per-requisition deviation from dept average.
        top_source_for_requisition
            Most common source across all applications on the
            requisition. Useful for the source-mix dashboard tab.
        top_source_share
            Share of total_applications attributed to top_source.
*/



with funnel as (
    select * from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
),

source_counts_per_req as (
    select
        requisition_id,
        source,
        count(*) as source_count
    from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
    group by requisition_id, source
),

top_source as (
    select
        requisition_id,
        source        as top_source_for_requisition,
        source_count  as top_source_count
    from source_counts_per_req
    qualify row_number() over (
        partition by requisition_id
        order by source_count desc, source
    ) = 1
),

with_dept_avgs_and_top_source as (
    select
        f.*,
        avg(f.time_to_fill_days)  over (partition by f.department) as dept_avg_time_to_fill_days,
        avg(f.time_to_offer_days) over (partition by f.department) as dept_avg_time_to_offer_days,
        avg(f.rate_overall_conversion) over (partition by f.department) as dept_avg_overall_conversion,
        ts.top_source_for_requisition,
        ts.top_source_count
    from funnel as f
    left join top_source as ts on f.requisition_id = ts.requisition_id
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

        -- Source mix
        top_source_for_requisition,
        top_source_count,
        round(safe_divide(top_source_count, total_applications), 4) as top_source_share
    from with_dept_avgs_and_top_source
)

select * from final
    );
  