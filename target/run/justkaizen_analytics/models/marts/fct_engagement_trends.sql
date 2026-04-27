
  
    

    create or replace table `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
      
    
    

    
    OPTIONS(
      description="""Theme-level engagement trends with company-wide comparison\ncolumns. ~896 rows: 16 cycles x 7 departments x 8 themes.\nis_post_layoff_period flag delineates the 2023-Q1+ cycles\nwhere Section 10's layoff-trauma signal is visible.\n"""
    )
    as (
      /*
    Model:        fct_engagement_trends
    Layer:        marts
    Sources:      int_engagement_theme_rollup
    Materialized: table  (per dbt_project.yml marts defaults)

    Purpose
    -------
    Theme-level engagement trends with company-wide comparison columns.
    One row per (survey_cycle, department, theme); ~896 rows
    (16 cycles x 7 departments x 8 themes).

    Calculated fields beyond int_engagement_theme_rollup
    ----------------------------------------------------
        cycle_year, cycle_quarter
            Time bucketing for filter / group-by in BI.

        theme_avg_favorable_pct_display
            theme_avg_favorable_pct * 100 (rounded), so dashboards
            can render percentages without re-multiplying.

        company_theme_avg_score
            Average theme_avg_score for that (cycle, theme) across
            all 7 departments. Window aggregate.

        company_theme_avg_favorable_pct
            Same shape on favorable_pct.

        theme_score_vs_company_avg
            theme_avg_score minus company_theme_avg_score for the
            same (cycle, theme). Positive = department outperforms
            company on this theme.

        is_post_layoff_period
            TRUE on cycles 2023-Q1 onward (the post-layoff trauma
            window per Section 10). Useful for showing the dip.
*/



with rollup as (
    select * from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`
),

with_company_avgs as (
    select
        *,
        avg(theme_avg_score) over (
            partition by survey_cycle, theme
        ) as company_theme_avg_score,
        avg(theme_avg_favorable_pct) over (
            partition by survey_cycle, theme
        ) as company_theme_avg_favorable_pct
    from rollup
),

final as (
    select
        -- Cycle identity
        survey_cycle,
        cycle_end_date,
        extract(year    from cycle_end_date) as cycle_year,
        extract(quarter from cycle_end_date) as cycle_quarter,

        -- Dimensions
        department,
        sub_department,
        theme,
        questions_in_theme,
        response_count,

        -- Theme score + favorable
        theme_avg_score,
        theme_avg_favorable_pct,
        round(theme_avg_favorable_pct * 100, 1) as theme_avg_favorable_pct_display,
        enps_score,

        -- Company comparison
        round(company_theme_avg_score,         2) as company_theme_avg_score,
        round(company_theme_avg_favorable_pct, 4) as company_theme_avg_favorable_pct,
        round(theme_avg_score - company_theme_avg_score, 2) as theme_score_vs_company_avg,
        round(theme_avg_favorable_pct - company_theme_avg_favorable_pct, 4) as theme_favorable_pct_vs_company_avg,

        -- Trend
        prior_theme_avg_score,
        theme_score_delta,
        prior_theme_avg_favorable_pct,
        theme_favorable_pct_delta,

        -- Layoff window flag (Section 10 narrative: 2023-Q1 layoff hits;
        -- 2023-Q2 / Q3 trough; 2023-Q4 onward gradual recovery)
        case when survey_cycle >= '2023-Q1' then true else false end as is_post_layoff_period
    from with_company_avgs
)

select * from final
order by cycle_end_date, department, theme
    );
  