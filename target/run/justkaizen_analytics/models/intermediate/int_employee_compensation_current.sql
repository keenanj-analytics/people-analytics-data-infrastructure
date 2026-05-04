

  create or replace view `just-kaizen-ai`.`raw_intermediate`.`int_employee_compensation_current`
  OPTIONS()
  as /*
    Model:   int_employee_compensation_current
    Layer:   Intermediate (helper)
    Sources: stg_employees, stg_comp_bands
    Grain:   One row per employee
    PK:      employee_id

    Purpose:
        Latest salary per employee, matched to the appropriate comp band
        (Zone A or Zone B based on employee_zone). Computes compa_ratio.
        Feeds the roster.

    Logic:
        - Pick the most recent stg_employees snapshot per employee_id
          via ROW_NUMBER() OVER (PARTITION BY employee_id ORDER BY
          report_date DESC).
        - LEFT JOIN stg_comp_bands on job_title.
        - CASE on employee_zone: 'ZONE A' -> zone_a_*, 'ZONE B' -> zone_b_*.
          Anything else (NULL, unrecognized) yields NULL bands.
        - compa_ratio = ROUND(salary / comp_band_mid, 2), via SAFE_DIVIDE
          to guard against missing band rows.

    Notes:
        - Salary is sourced from stg_employees (passthrough from
          raw_employees). raw_employees gains a Salary column during the
          rescale phase.
        - Job titles in stg_employees that do not appear in stg_comp_bands
          will produce NULL band fields — surfaced rather than silently
          dropped so the roster can flag unmatched titles in tests.
*/

with latest_snapshot as (

    select *
    from (
        select
            employee_id,
            job_title,
            employee_zone,
            salary,
            row_number() over (
                partition by employee_id
                order by report_date desc
            ) as rn
        from `just-kaizen-ai`.`raw_staging`.`stg_employees`
    )
    where rn = 1

),

joined as (

    select
        e.employee_id,
        e.job_title,
        e.employee_zone,
        e.salary,

        case e.employee_zone
            when 'ZONE A' then b.zone_a_min_salary
            when 'ZONE B' then b.zone_b_min_salary
        end as comp_band_min,

        case e.employee_zone
            when 'ZONE A' then b.zone_a_mid_salary
            when 'ZONE B' then b.zone_b_mid_salary
        end as comp_band_mid,

        case e.employee_zone
            when 'ZONE A' then b.zone_a_max_salary
            when 'ZONE B' then b.zone_b_max_salary
        end as comp_band_max

    from latest_snapshot as e
    left join `just-kaizen-ai`.`raw_staging`.`stg_comp_bands` as b
        on e.job_title = b.job_title

),

final as (

    select
        employee_id,
        job_title,
        employee_zone,
        salary,
        comp_band_min,
        comp_band_mid,
        comp_band_max,
        round(safe_divide(salary, comp_band_mid), 2) as compa_ratio
    from joined

)

select * from final;

