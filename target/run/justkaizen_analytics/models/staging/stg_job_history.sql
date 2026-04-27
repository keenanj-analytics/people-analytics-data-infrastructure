

  create or replace view `just-kaizen-ai`.`raw_staging`.`stg_job_history`
  OPTIONS(
      description="""Staging view over raw_job_history (ADP). One row per career event\nper employee. Old/new fields capture the state on either side of\nthe change; old_* are null on Hire rows. Calculated fields\n(months_since_last_promotion, total_promotions,\nhas_manager_change_last_12mo, career_velocity) derive in\nintermediate / marts.\n"""
    )
  as /*
    Model:        stg_job_history
    Layer:        staging
    Source:       seeds/raw_job_history.csv  (ADP - Job Change History)
    Materialized: view  (per dbt_project.yml staging defaults)

    Purpose
    -------
    Type-cast and lightly clean the seeded raw_job_history CSV. One row per
    career event per employee, ordered chronologically. Output is a typed
    1:1 view of the source rows; no calculated time-to-promotion,
    career_velocity, total_promotions, or has_manager_change_last_12mo
    fields are introduced here -- those derive in intermediate / marts.

    Source dependencies
    -------------------
    - seeds/raw_job_history.csv (loaded into raw.raw_job_history by dbt seed)

    Business rules applied
    ----------------------
    - All old_* fields (old_job_level / old_department / old_sub_department
      / old_job_title / old_manager_id) are null on Hire rows -- the Hire
      event has no prior state.
    - Title Change rows in this dataset specifically encode the Manager
      Step-Back archetype's M1 -> IC4 demotion. The data dictionary's
      change_type domain has no native "Step-back" / "Demotion" value, so
      Title Change is the closest fit.
    - Sequential level progression on Promotion rows (one IC step at a
      time, IC -> M jumps directly to M1, M -> M one step at a time) is
      validated upstream by HR5 in Section 12 coherence.
    - Section 12 also enforces no events after termination_date (HR4),
      most-recent-event matches stg_employees current state (HR3), and
      Hire row's effective_date equals stg_employees.hire_date (HR1).
*/



with source as (
    select * from `just-kaizen-ai`.`raw_raw`.`raw_job_history`
),

renamed as (
    select
        -- Foreign key to stg_employees
        employee_id,

        -- Event identity
        effective_date,
        change_type,

        -- Level transition
        old_job_level,
        new_job_level,

        -- Department / sub-department transition
        old_department,
        new_department,
        old_sub_department,
        new_sub_department,

        -- Title transition
        old_job_title,
        new_job_title,

        -- Manager transition
        old_manager_id,
        new_manager_id
    from source
)

select * from renamed;

