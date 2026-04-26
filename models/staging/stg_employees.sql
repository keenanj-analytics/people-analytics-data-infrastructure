/*
    Model:        stg_employees
    Layer:        staging
    Source:       seeds/raw_employees.csv  (ADP - Core Employee Records)
    Materialized: view  (per dbt_project.yml staging defaults)

    Purpose
    -------
    Type-cast and lightly clean the seeded raw_employees CSV. Output is a
    1:1 typed view of the source rows. No business logic, joins, or
    calculated fields are introduced in staging; downstream models in
    intermediate/ and marts/ derive tenure_months, is_active, compa_ratio,
    has_recent_promotion, and other analyst-facing measures.

    Source dependencies
    -------------------
    - seeds/raw_employees.csv (loaded into the `raw` dataset by `dbt seed`)

    Business rules applied
    ----------------------
    - is_critical_talent receives an explicit BOOL cast (defensive; the
      seed column_types in dbt_project.yml already enforce BOOL on load,
      but the cast here documents intent).
    - hire_date and termination_date are passed through unchanged. Their
      type is fixed to DATE by the seed config.
    - All STRING fields pass through as-is. Section 12 cross-table
      coherence is validated in the upstream Python pipeline
      (scripts/data_generation/13_validate_and_export.py); this model
      trusts the loaded shape.
*/

{{ config(materialized='view') }}

with source as (
    select * from {{ ref('raw_employees') }}
),

renamed as (
    select
        -- Identifiers
        employee_id,
        manager_id,

        -- Personal
        first_name,
        last_name,
        email,

        -- Org placement
        department,
        sub_department,
        job_title,
        job_level,

        -- Employment timeline
        hire_date,
        termination_date,
        employment_status,
        termination_type,
        termination_reason,

        -- Demographics (DEIB reporting only)
        gender,
        race_ethnicity,

        -- Location (US-only, remote-first)
        location_city,
        location_state,

        -- Talent designation (People Ops flag, refreshed quarterly)
        cast(is_critical_talent as bool) as is_critical_talent
    from source
)

select * from renamed
