/*
    Model:        stg_performance
    Layer:        staging
    Source:       seeds/raw_performance.csv  (Lattice - Performance Reviews)
    Materialized: view  (per dbt_project.yml staging defaults)

    Purpose
    -------
    Type-cast and lightly clean the seeded raw_performance CSV. One row
    per (employee, review_cycle). Output is a typed 1:1 view of the
    source rows; calculated fields (rating_numeric, is_top_performer)
    derive in intermediate / marts where they can be reused.

    Source dependencies
    -------------------
    - seeds/raw_performance.csv (loaded into raw.raw_performance by dbt seed)

    Business rules applied
    ----------------------
    - All STRING / DATE columns pass through unchanged. self_rating is
      nullable per spec (not every employee submits a self assessment);
      every other column is non-null in this dataset.
    - The H1 / H2 cycle schedule (Jul 15 / Jan 15 +/- 7 days) and the
      hire-date + 90-day eligibility rule are validated upstream by
      Section 12 HR11 and HR13.
    - Promotion coherence (employee promoted at date X must show Exceeds
      or higher in the cycle ending immediately before X) is validated
      upstream by HR14. The post-process upgrade pass in
      10_build_raw_performance.py guarantees zero violations on the
      seeded data.

    Calculated fields routed downstream
    -----------------------------------
        rating_numeric    SE=5, E=4, M=3, PM=2, DNM=1 (per data dict)
        is_top_performer  TRUE when overall_rating IN ('Significantly
                          Exceeds', 'Exceeds') for 2 consecutive cycles
        cycle_end_date    parsed from review_cycle (e.g. '2024-H2' ->
                          2025-01-15) for join logic
*/



with source as (
    select * from `just-kaizen-ai`.`raw_raw`.`raw_performance`
),

renamed as (
    select
        -- Foreign key to stg_employees
        employee_id,

        -- Cycle identity
        review_cycle,
        review_completed_date,
        review_status,

        -- Ratings (same 5-point scale across all three)
        overall_rating,
        manager_rating,
        self_rating
    from source
)

select * from renamed