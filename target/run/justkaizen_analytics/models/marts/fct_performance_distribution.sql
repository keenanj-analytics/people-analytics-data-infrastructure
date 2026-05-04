
  
    

    create or replace table `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
      
    
    

    
    OPTIONS()
    as (
      /*
    Model:        fct_performance_distribution
    Layer:        Mart
    Materialized: table
    Grain:        One row per (employee_id × review_cycle)
    Sources:      stg_performance — every (employee, cycle) rating
                  stg_employees   — current org context (latest snapshot)

    Purpose:
        Per-employee per-cycle rating with org context. Drives the
        performance dashboard's distribution view (rating mix by
        department / level / level_group) and movement-between-cycles
        drill.

    Org context:
        Department, job_level, and level_group come from the latest
        stg_employees snapshot per employee — same V1 simplification
        as the roster. A 2024 review will display the employee's 2026
        department if they've transferred since the review.

    is_top_performer:
        "Y" if overall_rating_numeric >= 4 OR critical_talent = TRUE,
        else "N". Same rule as the roster's top_performer_flag, applied
        per review row rather than per month.
*/

with performance as (

    select * from `just-kaizen-ai`.`raw_staging`.`stg_performance`

),

employees_latest as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by employee_id
                order by report_date desc
            ) as rn
        from `just-kaizen-ai`.`raw_staging`.`stg_employees`
    )
    where rn = 1

),

final as (

    select
        p.employee_id,
        p.cycle_name                            as review_cycle,
        e.full_name,
        e.department,
        e.sub_department,
        e.job_title,
        e.job_level,
        case
            when e.job_level in ('P1','P2','P3')                then 'Junior IC'
            when e.job_level in ('P4','P5','P6')                then 'Senior IC'
            when e.job_level in ('M1','M2')                     then 'Manager'
            when e.job_level in ('M3','M4')                     then 'Director'
            when e.job_level in ('E1','E2','E3','E4','E5','E6') then 'Senior Leadership'
        end                                     as level_group,
        p.overall_rating,
        p.overall_rating_numeric,
        case
            when coalesce(p.overall_rating_numeric, 0) >= 4 then 'Y'
            when coalesce(e.critical_talent, false)         then 'Y'
            else 'N'
        end                                     as is_top_performer
    from performance as p
    left join employees_latest as e
        on p.employee_id = e.employee_id

)

select * from final
    );
  