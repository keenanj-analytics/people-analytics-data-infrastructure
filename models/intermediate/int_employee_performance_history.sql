/*
    Model:  int_employee_performance_history
    Layer:  Intermediate (helper)
    Source: stg_performance
    Grain:  One row per employee
    PK:     employee_id

    Purpose:
        Latest completed manager review per employee. Feeds the roster.

    Logic:
        - ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY cycle_name
          DESC) picks the most recent cycle. JustKaizen cycle naming
          ("YYYY Mid-Year Review Cycle" / "YYYY Year-End Review Cycle")
          sorts chronologically when sorted alphabetically descending —
          year prefix orders the years, then "Year-End" > "Mid-Year"
          alphabetically within a year.
        - top_performer_flag = "Y" if overall_rating_numeric >= 4, else "N".
          The "OR is_critical_talent" half of the rule is intentionally
          NOT applied here — is_critical_talent lives on stg_employees and
          is layered in by the roster, where both inputs are joined.

    Notes:
        - Employees who have never had a completed manager review are
          absent from this helper. The roster LEFT JOINs and surfaces them
          with NULL rating fields and top_performer_flag derived from
          critical_talent alone.
        - If the rescale phase introduces other cycle types (calibration,
          off-cycle reviews) that don't sort chronologically by string,
          replace the ORDER BY with an extracted (year, half) tuple.
*/

with ranked as (

    select
        employee_id,
        cycle_name,
        overall_rating,
        overall_rating_numeric,
        row_number() over (
            partition by employee_id
            order by cycle_name desc
        ) as rn
    from {{ ref('stg_performance') }}

),

latest as (

    select *
    from ranked
    where rn = 1

),

final as (

    select
        employee_id,
        cycle_name                  as latest_review_cycle,
        overall_rating              as latest_perf_rating,
        overall_rating_numeric      as latest_perf_rating_numeric,
        case
            when overall_rating_numeric >= 4 then 'Y'
            else 'N'
        end                         as top_performer_flag
    from latest

)

select * from final
