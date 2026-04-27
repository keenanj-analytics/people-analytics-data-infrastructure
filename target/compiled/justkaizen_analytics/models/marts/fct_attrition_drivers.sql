/*
    Model:        fct_attrition_drivers
    Layer:        marts
    Sources:      int_employee_dimension
                  int_employee_event_sequence
                  int_employee_performance_history
    Materialized: table  (per dbt_project.yml marts defaults)

    Purpose
    -------
    One row per terminated employee (~224 rows) with attribution
    columns the analytics team uses to explain why people leave:
    tenure at termination, time since last promotion, recent manager
    change, performance trajectory, and a heuristic
    `terminated_within_6mo_of_manager_change` flag (matches the
    Manager Change Casualty archetype's defining pattern per Section 5).

    Calculated fields
    -----------------
        tenure_at_termination_months    months from hire_date to
                                          termination_date
        last_manager_change_before_term  most recent Manager Change
                                          event whose effective_date <
                                          termination_date
        months_since_last_manager_change_at_term
                                          months from that event to
                                          termination_date
        terminated_within_6mo_of_manager_change
                                          TRUE if the gap above is <= 6
                                          months. Surfaces the Manager
                                          Change Casualty pattern.
        last_review_rating_at_term       overall_rating from the last
                                          review prior to termination
        last_review_rating_numeric_at_term
                                          numeric mapping
        was_declining_performer
                                          TRUE if the last review's
                                          overall_rating_delta < 0
                                          (numeric drop). Surfaces the
                                          Performance Managed Out
                                          pattern.
*/



with terminated_employees as (
    select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
    where employment_status = 'Terminated'
),

manager_change_events as (
    select *
    from `just-kaizen-ai`.`raw_intermediate`.`int_employee_event_sequence`
    where change_type = 'Manager Change'
),

last_manager_change_per_employee as (
    select
        emp.employee_id,
        max(mc.effective_date) as last_manager_change_before_term
    from terminated_employees as emp
    left join manager_change_events as mc
      on emp.employee_id = mc.employee_id
     and mc.effective_date < emp.termination_date
    group by emp.employee_id
),

last_review_before_term as (
    select *
    from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history` as p
    where exists (
        select 1
        from terminated_employees as e
        where e.employee_id = p.employee_id
          and p.review_completed_date <= e.termination_date
    )
    qualify row_number() over (
        partition by employee_id
        order by review_completed_date desc, cycle_sequence desc
    ) = 1
),

final as (
    select
        -- Identity
        e.employee_id,
        e.first_name,
        e.last_name,

        -- Dept / level / demographics at termination snapshot
        e.department,
        e.sub_department,
        e.job_level,
        e.gender,
        e.race_ethnicity,
        e.location_state,
        e.is_critical_talent,

        -- Termination details
        e.hire_date,
        e.termination_date,
        e.termination_type,
        e.termination_reason,
        date_diff(e.termination_date, e.hire_date, month) as tenure_at_termination_months,
        round(date_diff(e.termination_date, e.hire_date, day) / 365.25, 2) as tenure_at_termination_years,

        -- Promotion context
        e.total_promotions,
        e.last_promotion_date,
        e.months_since_last_promotion as months_since_last_promotion_at_term,

        -- Manager change context
        mcr.last_manager_change_before_term,
        case
            when mcr.last_manager_change_before_term is not null
                then date_diff(e.termination_date, mcr.last_manager_change_before_term, month)
            else null
        end as months_since_last_manager_change_at_term,
        case
            when mcr.last_manager_change_before_term is not null
                and date_diff(e.termination_date, mcr.last_manager_change_before_term, month) <= 6
                then true
            else false
        end as terminated_within_6mo_of_manager_change,

        -- Performance context
        lrb.review_cycle           as last_review_cycle_at_term,
        lrb.overall_rating         as last_review_rating_at_term,
        lrb.overall_rating_numeric as last_review_rating_numeric_at_term,
        lrb.overall_rating_delta   as last_review_rating_delta_at_term,
        case
            when lrb.overall_rating_delta is not null
                and lrb.overall_rating_delta < 0
                then true
            else false
        end as was_declining_performer,

        -- Comp at last record (forward-carried in int_employee_dimension)
        e.salary,
        e.compa_ratio,
        e.band_position
    from terminated_employees                  as e
    left join last_manager_change_per_employee as mcr on e.employee_id = mcr.employee_id
    left join last_review_before_term          as lrb on e.employee_id = lrb.employee_id
)

select * from final
order by termination_date desc