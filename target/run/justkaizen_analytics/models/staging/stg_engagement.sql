

  create or replace view `just-kaizen-ai`.`raw_staging`.`stg_engagement`
  OPTIONS(
      description="""Staging view over raw_engagement (Lattice, ANONYMIZED). One row\nper (survey_cycle, department, sub_department, question_id).\nNo employee_id field by design -- all metrics aggregated at the\ndepartment level to preserve anonymity. The CEO (Maya, Executive\ndepartment) is excluded from aggregation since the dept has only\none employee. Calculated rollups (theme averages, cycle-over-\ncycle deltas, top-quartile flags) derive in intermediate / marts.\n"""
    )
  as /*
    Model:        stg_engagement
    Layer:        staging
    Source:       seeds/raw_engagement.csv  (Lattice - Engagement Survey, ANONYMIZED)
    Materialized: view  (per dbt_project.yml staging defaults)

    Purpose
    -------
    Type-cast and lightly clean the seeded raw_engagement CSV. One row
    per (survey_cycle, department, sub_department, question_id). Output
    is a typed 1:1 view of the source rows; calculated theme rollups,
    cycle-over-cycle trend deltas, and question rollups derive in
    intermediate / marts.

    Privacy
    -------
    This table is anonymized -- no employee_id field exists. All metrics
    are aggregated at the (cycle, department) granularity, with optional
    sub-department detail when the sub-dept has 5+ respondents per
    Section 12 anonymity rule. Sub-department rows are not generated in
    this dataset; the column is present for forward compatibility.

    Source dependencies
    -------------------
    - seeds/raw_engagement.csv (loaded into raw.raw_engagement by dbt seed)

    Business rules applied
    ----------------------
    - Maya Chen (the sole Executive department employee) is excluded
      from the per-(cycle, department) aggregation by design --
      reporting on a single person would violate anonymization.
    - response_count is selected as INT64 to fall in the integer range
      [ceil(0.78 * active_count), floor(0.88 * active_count)] per
      Section 12 HR20. For the smallest departments where the integer
      rate is unattainable, the closest integer to 0.83 * active_count
      is used.
    - enps_score is the same value across every row that shares
      (survey_cycle, department) -- denormalized per the schema.
    - Section 12 HR19 enforces response_count <= active dept headcount;
      HR20 enforces the 78-88% rate.

    Calculated fields routed downstream
    -----------------------------------
        favorable_pct_pct       favorable_pct * 100 for percentage display
        score_vs_baseline       avg_score - company-wide baseline (Section 10)
        cycle_over_cycle_delta  current cycle's avg_score minus prior cycle's
        is_top_quartile         within-theme percentile flag
*/



with source as (
    select * from `just-kaizen-ai`.`raw_raw`.`raw_engagement`
),

renamed as (
    select
        -- Cycle + dimension identity
        survey_cycle,
        department,
        sub_department,

        -- Question identity
        question_id,
        question_text,
        theme,

        -- Aggregate metrics
        avg_score,
        favorable_pct,
        response_count,

        -- Department-level eNPS (denormalized -- same value across all
        -- rows that share survey_cycle + department)
        enps_score
    from source
)

select * from renamed;

