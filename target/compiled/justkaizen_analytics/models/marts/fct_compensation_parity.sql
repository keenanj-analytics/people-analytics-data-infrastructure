/*
    Model:        fct_compensation_parity
    Layer:        marts
    Sources:      int_employee_dimension
    Materialized: table  (per dbt_project.yml marts defaults)

    Purpose
    -------
    Per-active-employee compensation snapshot with peer-group
    percentiles. Granularity: one row per active employee. The peer
    group for ranking is (department, job_level) -- the natural
    cohort within which compa_ratio comparison is fair.

    Filters
    -------
    Active employees only. Comp parity for terminated employees
    isn't a useful analytical view; the underlying comp history is
    still in stg_compensation if needed.

    Calculated fields (added on top of int_employee_dimension)
    ----------------------------------------------------------
        compa_ratio_percentile_within_dept_level
            PERCENT_RANK over (department, job_level). Range [0, 1].
            0 = lowest compa_ratio in cohort; 1 = highest.

        salary_percentile_within_dept_level
            Same shape on raw salary.

        cohort_avg_compa_ratio                  AVG over (dept, level)
        cohort_avg_salary                       AVG over (dept, level)
        cohort_size                             COUNT over (dept, level)

        compa_ratio_vs_cohort_avg               compa_ratio - cohort_avg_compa_ratio
        salary_vs_cohort_avg                    salary - cohort_avg_salary

        compa_ratio_quartile_label
            'Bottom Quartile' / 'Q2' / 'Q3' / 'Top Quartile' from the
            percentile.

    These let dashboards pivot on (gender, race_ethnicity, location_state)
    within the (dept, level) cohort to surface pay-parity gaps without
    further aggregation.
*/



with active_employees as (
    select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
    where is_active
),

with_cohort_metrics as (
    select
        *,
        percent_rank() over (
            partition by department, job_level
            order by compa_ratio
        ) as compa_ratio_percentile_within_dept_level,

        percent_rank() over (
            partition by department, job_level
            order by salary
        ) as salary_percentile_within_dept_level,

        avg(compa_ratio) over (
            partition by department, job_level
        ) as cohort_avg_compa_ratio,

        avg(salary) over (
            partition by department, job_level
        ) as cohort_avg_salary,

        count(*) over (
            partition by department, job_level
        ) as cohort_size
    from active_employees
),

final as (
    select
        -- Identity
        employee_id,
        first_name,
        last_name,
        email,

        -- Cohort dimensions
        department,
        sub_department,
        job_level,
        job_title,

        -- Demographics for parity slicing
        gender,
        race_ethnicity,
        location_city,
        location_state,
        is_critical_talent,

        -- Tenure context
        hire_date,
        tenure_years,
        total_promotions,

        -- Comp snapshot
        salary,
        comp_band_min,
        comp_band_mid,
        comp_band_max,
        compa_ratio,
        band_position,
        current_comp_change_reason,
        current_comp_effective_date,

        -- Peer-cohort rank
        round(compa_ratio_percentile_within_dept_level, 4) as compa_ratio_percentile_within_dept_level,
        round(salary_percentile_within_dept_level,      4) as salary_percentile_within_dept_level,
        round(cohort_avg_compa_ratio, 4)                   as cohort_avg_compa_ratio,
        round(cohort_avg_salary,      0)                   as cohort_avg_salary,
        cohort_size,

        -- Distance from cohort average
        round(compa_ratio - cohort_avg_compa_ratio, 4) as compa_ratio_vs_cohort_avg,
        round(salary - cohort_avg_salary,           0) as salary_vs_cohort_avg,

        case
            when compa_ratio_percentile_within_dept_level >= 0.75 then 'Top Quartile'
            when compa_ratio_percentile_within_dept_level >= 0.50 then 'Q3'
            when compa_ratio_percentile_within_dept_level >= 0.25 then 'Q2'
            else 'Bottom Quartile'
        end as compa_ratio_quartile_label
    from with_cohort_metrics
)

select * from final