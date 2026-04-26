/*
    Model:        int_employee_event_sequence
    Layer:        intermediate
    Sources:      stg_job_history
    Materialized: view  (per dbt_project.yml intermediate defaults)

    Purpose
    -------
    Chronologically ordered career-event stream with prev / next event
    metadata per row. One row per (employee_id, event) -- the same
    granularity as stg_job_history -- enriched with window calculations
    that downstream marts and the org-evolution dashboard need:

    - event_sequence:           1-based ordinal per employee (Hire = 1)
    - is_latest_event:          flag on the most recent row per employee
    - prev_event_date / type:   the immediately preceding event
    - next_event_date / type:   the immediately following event
    - days_since_prev_event:    interval to the previous event
    - days_until_next_event:    interval to the next event

    Tiebreaker on same-date events
    ------------------------------
    Ordering uses (effective_date, change_type) so same-day events are
    deterministic. 'Hire' sorts first alphabetically -- which is the
    correct semantic placement since the Hire event is always the first
    event for an employee. Other same-day events (Promotion + Manager
    Change is the most common collision) get an alphabetical order;
    this is arbitrary but stable across runs.

    What this model does NOT compute
    --------------------------------
    Per-employee aggregates (total_promotions, last_promotion_date,
    has_manager_change_last_12mo, career_velocity_per_year) live in
    int_employee_tenure. This model preserves row-level granularity so
    downstream consumers can filter / pivot to whichever event subset
    they need.
*/

{{ config(materialized='view') }}

with events as (
    select * from {{ ref('stg_job_history') }}
),

with_ordering as (
    select
        *,
        row_number() over (
            partition by employee_id
            order by effective_date, change_type
        ) as event_sequence,
        count(*) over (
            partition by employee_id
        ) as total_events_for_employee
    from events
),

with_neighbors as (
    select
        *,

        -- Previous event
        lag(effective_date) over (
            partition by employee_id
            order by event_sequence
        ) as prev_event_date,
        lag(change_type) over (
            partition by employee_id
            order by event_sequence
        ) as prev_event_type,

        -- Next event
        lead(effective_date) over (
            partition by employee_id
            order by event_sequence
        ) as next_event_date,
        lead(change_type) over (
            partition by employee_id
            order by event_sequence
        ) as next_event_type
    from with_ordering
),

final as (
    select
        -- Identity & ordering
        employee_id,
        effective_date,
        change_type,
        event_sequence,
        total_events_for_employee,
        case
            when event_sequence = total_events_for_employee then true
            else false
        end as is_latest_event,

        -- Old / new state (carried forward from stg_job_history)
        old_job_level,
        new_job_level,
        old_department,
        new_department,
        old_sub_department,
        new_sub_department,
        old_job_title,
        new_job_title,
        old_manager_id,
        new_manager_id,

        -- Neighbor metadata
        prev_event_date,
        prev_event_type,
        next_event_date,
        next_event_type,

        -- Time gaps
        case
            when prev_event_date is not null
                then date_diff(effective_date, prev_event_date, day)
            else null
        end as days_since_prev_event,
        case
            when next_event_date is not null
                then date_diff(next_event_date, effective_date, day)
            else null
        end as days_until_next_event
    from with_neighbors
)

select * from final
