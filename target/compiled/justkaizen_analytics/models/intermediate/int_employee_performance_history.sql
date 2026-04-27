/*
    Model:        int_employee_performance_history
    Layer:        intermediate
    Sources:      stg_performance
    Materialized: view  (per dbt_project.yml intermediate defaults)

    Purpose
    -------
    Per-cycle performance history with the spec's two calculated fields
    plus auxiliary trend signals downstream consumers will need. One row
    per (employee_id, review_cycle), preserving the full history. Marts
    that only need the latest row per employee can filter on
    cycle_sequence = max(cycle_sequence) over (...) downstream.

    Calculated fields
    -----------------
        rating_numeric (per rating column)
            SE=5, E=4, M=3, PM=2, DNM=1 per data dict. Computed for
            overall_rating, manager_rating, and self_rating
            independently. Self_rating_numeric is null when self_rating
            is null.

        cycle_end_date
            Parsed from review_cycle (e.g. '2024-H2' -> 2025-01-15).
            H1 cycles complete July 15 of the same year; H2 cycles
            complete January 15 of the following year (per Section 7).

        cycle_sequence
            1-based ordinal of the cycle within the employee's review
            history. Ordered by cycle_end_date ascending. The first
            review the employee was eligible for is cycle_sequence = 1.

        prior_overall_rating, prior_overall_rating_numeric
            The overall_rating from the immediately previous cycle for
            this employee. Null on cycle_sequence = 1.

        overall_rating_delta
            overall_rating_numeric minus prior_overall_rating_numeric.
            Positive = improving, negative = declining. Null on
            cycle_sequence = 1.

        is_top_performer (per data dict)
            TRUE when overall_rating IN ('Significantly Exceeds',
            'Exceeds') AND prior_overall_rating IN the same set. The
            "2 consecutive cycles" definition. Null cannot occur --
            cycles without a prior rating evaluate to FALSE.

        self_optimism_delta
            self_rating_numeric minus overall_rating_numeric. Positive
            indicates the employee rated themselves higher than the
            calibrated overall. Null when self_rating is null.

    Note on review_status
    ---------------------
    The calculations apply uniformly across review_status. Marts that
    want to exclude Exempt (M5 leadership) or Incomplete reviews from
    headline performance metrics should filter at consumption time.
*/



with perf as (
    select * from `just-kaizen-ai`.`raw_staging`.`stg_performance`
),

with_numerics as (
    select
        *,

        -- Rating to numeric (SE=5, E=4, M=3, PM=2, DNM=1)
        case overall_rating
            when 'Significantly Exceeds' then 5
            when 'Exceeds'                then 4
            when 'Meets'                  then 3
            when 'Partially Meets'        then 2
            when 'Does Not Meet'          then 1
        end as overall_rating_numeric,

        case manager_rating
            when 'Significantly Exceeds' then 5
            when 'Exceeds'                then 4
            when 'Meets'                  then 3
            when 'Partially Meets'        then 2
            when 'Does Not Meet'          then 1
        end as manager_rating_numeric,

        case self_rating
            when 'Significantly Exceeds' then 5
            when 'Exceeds'                then 4
            when 'Meets'                  then 3
            when 'Partially Meets'        then 2
            when 'Does Not Meet'          then 1
        end as self_rating_numeric,

        -- Cycle string -> date. H1 ends July 15 same year; H2 ends Jan 15 of the following year.
        case right(review_cycle, 2)
            when 'H1' then date(cast(left(review_cycle, 4) as int64),     7, 15)
            when 'H2' then date(cast(left(review_cycle, 4) as int64) + 1, 1, 15)
        end as cycle_end_date
    from perf
),

with_ordering as (
    select
        *,

        -- Per-employee chronological ordering
        row_number() over (
            partition by employee_id
            order by cycle_end_date
        ) as cycle_sequence,

        -- Prior cycle's rating (lag)
        lag(overall_rating) over (
            partition by employee_id
            order by cycle_end_date
        ) as prior_overall_rating,

        lag(overall_rating_numeric) over (
            partition by employee_id
            order by cycle_end_date
        ) as prior_overall_rating_numeric
    from with_numerics
),

final as (
    select
        -- Identity
        employee_id,
        review_cycle,
        cycle_end_date,
        cycle_sequence,

        -- Cycle metadata
        review_completed_date,
        review_status,

        -- Ratings (string + numeric pairs)
        overall_rating,
        overall_rating_numeric,
        manager_rating,
        manager_rating_numeric,
        self_rating,
        self_rating_numeric,

        -- Trend signals
        prior_overall_rating,
        prior_overall_rating_numeric,
        case
            when prior_overall_rating_numeric is not null
                then overall_rating_numeric - prior_overall_rating_numeric
            else null
        end as overall_rating_delta,

        -- Spec calculated field: 2-consecutive-cycle top performer
        case
            when overall_rating       in ('Significantly Exceeds', 'Exceeds')
             and prior_overall_rating in ('Significantly Exceeds', 'Exceeds')
                then true
            else false
        end as is_top_performer,

        -- Self-assessment optimism gap
        case
            when self_rating_numeric is not null
                then self_rating_numeric - overall_rating_numeric
            else null
        end as self_optimism_delta
    from with_ordering
)

select * from final