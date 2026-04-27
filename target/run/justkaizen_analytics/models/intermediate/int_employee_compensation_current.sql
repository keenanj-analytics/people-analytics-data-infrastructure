

  create or replace view `just-kaizen-ai`.`raw_intermediate`.`int_employee_compensation_current`
  OPTIONS(
      description="""One row per employee with their most-recent compensation record\nplus the spec's two calculated metrics (compa_ratio and\nband_position). \"Most recent\" is the row with the latest\neffective_date in stg_compensation; for terminated employees this\nis whatever record was active at termination (Section 12 HR8\nprohibits records after termination_date).\n"""
    )
  as /*
    Model:        int_employee_compensation_current
    Layer:        intermediate
    Sources:      stg_compensation
    Materialized: view  (per dbt_project.yml intermediate defaults)

    Purpose
    -------
    One row per employee with their most recent compensation snapshot
    plus the spec's two calculated comp metrics. "Most recent" means
    the row with the latest effective_date in stg_compensation; for
    terminated employees this is whatever record was active at
    termination, since Section 12 HR8 prohibits comp records after
    termination_date.

    Calculated fields
    -----------------
        compa_ratio    salary / comp_band_mid (per data dict)
                       Rounded to 4 decimals. Typical range 0.85 - 1.20
                       since salary is clipped to [band_min, band_max].
        band_position  (salary - comp_band_min) / (comp_band_max - comp_band_min)
                       Range [0, 1]. 0 = at band minimum, 1 = at band ceiling.
                       Uses SAFE_DIVIDE to handle the edge case where
                       comp_band_min == comp_band_max (no rows in this
                       dataset, but defensive).

    Edge cases
    ----------
    - Employees with a single comp record (the New Hire row) get that
      row as "latest" -- no ambiguity.
    - Same-day comp records (e.g. Promotion + Annual Review on the same
      Jan 15) are deduplicated by row_number with a deterministic
      tiebreaker on change_reason for reproducibility.
*/



with comp as (
    select * from `just-kaizen-ai`.`raw_staging`.`stg_compensation`
),

ranked as (
    select
        *,
        row_number() over (
            partition by employee_id
            order by effective_date desc, change_reason
        ) as rn
    from comp
),

latest as (
    select * except (rn)
    from ranked
    where rn = 1
),

final as (
    select
        employee_id,

        -- Provenance of the snapshot
        effective_date as current_comp_effective_date,
        change_reason  as current_comp_change_reason,

        -- Compensation snapshot
        salary,
        comp_band_min,
        comp_band_mid,
        comp_band_max,

        -- Calculated metrics
        round(salary / comp_band_mid, 4) as compa_ratio,
        round(
            safe_divide(salary - comp_band_min, comp_band_max - comp_band_min),
            4
        ) as band_position
    from latest
)

select * from final;

