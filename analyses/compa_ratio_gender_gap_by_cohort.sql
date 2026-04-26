/*
    Analysis:  Compa-ratio gender gap by (department, job_level) cohort
    Mart:      fct_compensation_parity
    Audience:  Total Rewards / DEIB review

    Question
    --------
    "Within each (department, job_level) cohort that has at least 10
    Male and 10 Female employees, what is the gender gap in average
    compa_ratio? Surface the largest gaps so we can investigate."

    Why within-cohort
    -----------------
    A raw "average compa_ratio by gender across the company" is biased
    by org composition (e.g., Engineering pays more and skews male).
    Comparing within (department, job_level) controls for the cohort
    so the gap reflects pay differences for similar work, not job-mix
    differences.

    Threshold rationale
    -------------------
    Cohorts smaller than 10-per-gender are excluded. With small samples
    individual outliers dominate; a 10-employee floor is a common
    convention in pay-equity audits.

    Result shape
    ------------
    One row per (department, job_level) cohort that meets the size
    threshold, with male / female / non-binary average compa_ratios,
    the male - female gap, and the cohort sample size.
*/

with by_cohort_gender as (
    select
        department,
        job_level,
        gender,
        count(*)         as headcount,
        avg(compa_ratio) as avg_compa_ratio
    from {{ ref('fct_compensation_parity') }}
    where gender is not null
    group by department, job_level, gender
),

pivoted as (
    select
        department,
        job_level,

        -- Headcounts
        sum(case when gender = 'Male'       then headcount end) as male_n,
        sum(case when gender = 'Female'     then headcount end) as female_n,
        sum(case when gender = 'Non-Binary' then headcount end) as non_binary_n,

        -- Average compa-ratios
        round(sum(case when gender = 'Male'       then avg_compa_ratio end), 4) as male_avg_compa_ratio,
        round(sum(case when gender = 'Female'     then avg_compa_ratio end), 4) as female_avg_compa_ratio,
        round(sum(case when gender = 'Non-Binary' then avg_compa_ratio end), 4) as non_binary_avg_compa_ratio
    from by_cohort_gender
    group by department, job_level
),

with_gap as (
    select
        *,
        round(male_avg_compa_ratio - female_avg_compa_ratio, 4) as male_minus_female_gap,
        round(
            safe_divide(male_avg_compa_ratio - female_avg_compa_ratio, female_avg_compa_ratio) * 100,
            2
        ) as gap_pct_of_female_baseline
    from pivoted
)

select *
from with_gap
where male_n     >= 10
  and female_n   >= 10
order by abs(male_minus_female_gap) desc
