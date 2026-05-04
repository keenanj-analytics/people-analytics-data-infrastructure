
  
    

    create or replace table `just-kaizen-ai`.`raw_marts`.`fct_attrition_reporting`
      
    
    

    
    OPTIONS()
    as (
      /*
    Model:        fct_attrition_reporting
    Layer:        Mart
    Materialized: table (mart-layer default)
    Grain:        report_month × department × sub_department × job_level
                  × level_group × tenure_bucket × top_performer_flag
                  × gender × race_ethnicity
    Source:       int_reporting_grid_attrition (scaffold)
                  int_employee_monthly_roster (activity)

    Purpose:
        The attrition reporting table Tableau reads. Carries headcount,
        termination counts (total / voluntary / involuntary / top performer
        / regrettable), TTM rolling totals, TTM rates, and three levels of
        benchmark (segment, department, company-wide) on every row.

    Construction:
        1. Aggregate roster by the 8 grid dimensions × report_month into
           cell_aggregated. Headcount = active at month end. Termination
           counts gated by is_attrition_eligible_term so excluded reasons
           (RIF, internships, etc.) drop out.
        2. LEFT JOIN cell_aggregated onto the grid via IS NOT DISTINCT
           FROM (so NULL dimension combos match correctly). COALESCE NULLs
           to 0 so trend lines are continuous.
        3. Compute TTM rolling windows per cell: SUM termination counts
           and AVG end_month_headcount across the 12-month trailing
           window. Rates = SAFE_DIVIDE(ttm_count, ttm_avg_headcount).
        4. Compute org-wide and department benchmark rates from the full
           roster (separate aggregations, partitioned only by month and
           by month+department respectively). Joined onto every cell row
           so every segment carries its parent benchmarks.

    Notes:
        - TTM denominator is AVG of 12 monthly end-headcounts, not a
          two-point average. This smooths RIF distortion per CLAUDE.md.
        - Top performer / regrettable TTM termination counts are computed
          as intermediates but not exposed as columns; the data dictionary
          surfaces only the rates. Add columns if needed downstream.
        - All aggregations (cell, org-wide, department) filter to
          employment_type = 'Full Time'. Part-time and contractor rows
          are excluded from every workforce metric in the warehouse.
        - top_performer_terminations is technically redundant when
          top_performer_flag is a grid dimension (= total_terminations
          in 'Y' cells, 0 in 'N' cells), but exposed per the dictionary
          for consistency with marts that don't dimension on it.
*/

with roster as (

    select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_monthly_roster`

),

cell_aggregated as (

    -- Per-cell counts (8 dimensions × month)
    select
        report_month,
        department,
        sub_department,
        job_level,
        level_group,
        tenure_bucket,
        top_performer_flag,
        gender,
        race_ethnicity,
        countif(employment_status = 'Active')                                       as end_month_headcount,
        countif(is_attrition_eligible_term)                                         as total_terminations,
        countif(is_attrition_eligible_term and termination_type = 'Voluntary')      as voluntary_terminations,
        countif(is_attrition_eligible_term and termination_type = 'Involuntary')    as involuntary_terminations,
        countif(is_attrition_eligible_term and top_performer_flag = 'Y')            as top_performer_terminations,
        countif(is_attrition_eligible_term and is_regrettable_termination = 'Regrettable')
                                                                                    as regrettable_terminations
    from roster
    where employment_type = 'Full Time'
    group by 1, 2, 3, 4, 5, 6, 7, 8, 9

),

scaffolded as (

    -- Grid LEFT JOIN cell_aggregated, COALESCE NULLs to 0
    select
        g.report_month,
        g.report_quarter,
        g.department,
        g.sub_department,
        g.job_level,
        g.level_group,
        g.tenure_bucket,
        g.top_performer_flag,
        g.gender,
        g.race_ethnicity,
        coalesce(a.end_month_headcount, 0)         as end_month_headcount,
        coalesce(a.total_terminations, 0)          as total_terminations,
        coalesce(a.voluntary_terminations, 0)      as voluntary_terminations,
        coalesce(a.involuntary_terminations, 0)    as involuntary_terminations,
        coalesce(a.top_performer_terminations, 0)  as top_performer_terminations,
        coalesce(a.regrettable_terminations, 0)    as regrettable_terminations
    from `just-kaizen-ai`.`raw_intermediate`.`int_reporting_grid_attrition` as g
    left join cell_aggregated as a
        on  g.report_month       =                a.report_month
        and g.department         is not distinct from a.department
        and g.sub_department     is not distinct from a.sub_department
        and g.job_level          is not distinct from a.job_level
        and g.level_group        is not distinct from a.level_group
        and g.tenure_bucket      is not distinct from a.tenure_bucket
        and g.top_performer_flag is not distinct from a.top_performer_flag
        and g.gender             is not distinct from a.gender
        and g.race_ethnicity     is not distinct from a.race_ethnicity

),

with_ttm as (

    -- Per-cell TTM rolling windows (12-month trailing)
    select
        *,
        sum(total_terminations)         over cell_window as ttm_total_terminations,
        sum(voluntary_terminations)     over cell_window as ttm_voluntary_terminations,
        sum(top_performer_terminations) over cell_window as ttm_top_performer_terminations,
        sum(regrettable_terminations)   over cell_window as ttm_regrettable_terminations,
        avg(end_month_headcount)        over cell_window as ttm_avg_headcount
    from scaffolded
    window cell_window as (
        partition by
            department,
            sub_department,
            job_level,
            level_group,
            tenure_bucket,
            top_performer_flag,
            gender,
            race_ethnicity
        order by report_month
        rows between 11 preceding and current row
    )

),

orgwide_monthly as (

    -- Company-wide counts per month
    select
        report_month,
        countif(employment_status = 'Active')                                       as end_month_headcount,
        countif(is_attrition_eligible_term)                                         as total_terminations,
        countif(is_attrition_eligible_term and termination_type = 'Voluntary')      as voluntary_terminations
    from roster
    where employment_type = 'Full Time'
    group by report_month

),

orgwide_ttm as (

    -- Company-wide TTM rates per month
    select
        report_month,
        safe_divide(
            sum(total_terminations)     over orgwide_window,
            avg(end_month_headcount)    over orgwide_window
        ) as orgwide_ttm_overall_attrition_rate,
        safe_divide(
            sum(voluntary_terminations) over orgwide_window,
            avg(end_month_headcount)    over orgwide_window
        ) as orgwide_ttm_voluntary_attrition_rate
    from orgwide_monthly
    window orgwide_window as (
        order by report_month
        rows between 11 preceding and current row
    )

),

dept_monthly as (

    -- Per-department counts per month
    select
        report_month,
        department,
        countif(employment_status = 'Active') as end_month_headcount,
        countif(is_attrition_eligible_term)   as total_terminations
    from roster
    where employment_type = 'Full Time'
    group by report_month, department

),

dept_ttm as (

    -- Per-department TTM overall attrition rate per month
    select
        report_month,
        department,
        safe_divide(
            sum(total_terminations)  over dept_window,
            avg(end_month_headcount) over dept_window
        ) as dept_ttm_overall_attrition_rate
    from dept_monthly
    window dept_window as (
        partition by department
        order by report_month
        rows between 11 preceding and current row
    )

),

final as (

    select
        -- Time
        t.report_month,
        t.report_quarter,
        case
            when extract(month from t.report_month) in (3, 6, 9, 12)        then true
            when t.report_month = max(t.report_month) over ()               then true
            else false
        end                                                                 as flag_end_of_quarter,
        t.report_month = max(t.report_month) over ()                        as flag_latest_report,

        -- Dimensions
        t.department,
        t.sub_department,
        t.job_level,
        t.level_group,
        t.tenure_bucket,
        t.top_performer_flag,
        t.gender,
        t.race_ethnicity,

        -- Cell-level monthly counts
        t.end_month_headcount,
        t.total_terminations,
        t.voluntary_terminations,
        t.involuntary_terminations,
        t.top_performer_terminations,
        t.regrettable_terminations,

        -- Cell-level TTM counts and rates
        t.ttm_total_terminations,
        t.ttm_voluntary_terminations,
        t.ttm_avg_headcount,
        safe_divide(t.ttm_total_terminations,          t.ttm_avg_headcount) as ttm_overall_attrition_rate,
        safe_divide(t.ttm_voluntary_terminations,      t.ttm_avg_headcount) as ttm_voluntary_attrition_rate,
        safe_divide(t.ttm_top_performer_terminations,  t.ttm_avg_headcount) as ttm_top_performer_attrition_rate,
        safe_divide(t.ttm_regrettable_terminations,    t.ttm_avg_headcount) as ttm_regrettable_attrition_rate,

        -- Benchmarks
        o.orgwide_ttm_overall_attrition_rate,
        o.orgwide_ttm_voluntary_attrition_rate,
        d.dept_ttm_overall_attrition_rate

    from with_ttm as t
    left join orgwide_ttm as o
        on t.report_month = o.report_month
    left join dept_ttm as d
        on  t.report_month = d.report_month
        and t.department is not distinct from d.department

)

select * from final
    );
  