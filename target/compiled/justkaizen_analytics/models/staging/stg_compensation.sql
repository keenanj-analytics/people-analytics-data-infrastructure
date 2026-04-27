/*
    Model:        stg_compensation
    Layer:        staging
    Source:       seeds/raw_compensation.csv  (Pave - Compensation Data)
    Materialized: view  (per dbt_project.yml staging defaults)

    Purpose
    -------
    Type-cast and lightly clean the seeded raw_compensation CSV. One row
    per comp event per employee. Output is a typed 1:1 view of the source
    rows; calculated compa_ratio and band_position derive in intermediate
    / marts where they can be reused across consumers.

    Source dependencies
    -------------------
    - seeds/raw_compensation.csv (loaded into raw.raw_compensation by dbt seed)

    Business rules applied
    ----------------------
    - All four salary columns (salary, comp_band_min/mid/max) are FLOAT64
      per spec. The Python pipeline rounds salary to the nearest $500 but
      the type stays FLOAT to match the data dictionary.
    - Section 12 HR7 enforces salary in [comp_band_min, comp_band_max]
      upstream; HR6 enforces a New Hire row per employee with
      effective_date = stg_employees.hire_date; HR8 enforces no comp
      records after termination_date; HR9 enforces every Promotion row
      in raw_job_history has a matching Pave row on the same date; HR10
      enforces no orphan comp record on Lateral Transfer / Manager
      Change events. None of those are re-checked here -- staging trusts
      the loaded shape.

    Per-event semantics
    -------------------
    change_reason values present in this dataset:
        New Hire           one per employee, effective_date = hire_date
        Promotion          matches every Promotion row in raw_job_history
        Annual Review      Jan 15 each year of tenure (skips < 6mo and
                            Promotion years)
        Market Adjustment  2023-09-01 for every employee active that day
        Equity Adjustment  reserved by spec; not generated in this dataset
*/



with source as (
    select * from `just-kaizen-ai`.`raw_raw`.`raw_compensation`
),

renamed as (
    select
        -- Foreign key to stg_employees
        employee_id,

        -- Event identity
        effective_date,
        change_reason,

        -- Compensation snapshot at this effective_date
        salary,
        comp_band_min,
        comp_band_mid,
        comp_band_max
    from source
)

select * from renamed