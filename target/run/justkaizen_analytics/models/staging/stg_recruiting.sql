

  create or replace view `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
  OPTIONS(
      description="""Staging view over raw_recruiting (Ashby). One row per candidate\napplication across ~592 requisitions. The 12 Q1 2020 hires\n(Maya, David, Priya, plus 9 founders) have no requisition by\ndesign -- they predate the company's ATS adoption per the spec\nnarrative. Calculated funnel metrics (time_to_fill, time_to_offer,\nper-stage conversion rates) derive in intermediate / marts.\n"""
    )
  as /*
    Model:        stg_recruiting
    Layer:        staging
    Source:       seeds/raw_recruiting.csv  (Ashby - Recruiting Pipeline)
    Materialized: view  (per dbt_project.yml staging defaults)

    Purpose
    -------
    Type-cast and lightly clean the seeded raw_recruiting CSV. One row
    per candidate application (~9,300 rows across ~592 requisitions).
    Output is a typed 1:1 view of the source rows; calculated funnel
    metrics (time_to_fill, time_to_offer, stage conversion rates) derive
    in intermediate / marts.

    Source dependencies
    -------------------
    - seeds/raw_recruiting.csv (loaded into raw.raw_recruiting by dbt seed)

    Business rules applied
    ----------------------
    - This dataset only contains terminal-state rows: current_stage is
      always Hired, Rejected, or Withdrawn. The data dictionary's full
      domain (Applied / Phone Screen / Technical / Onsite / Offer /
      Hired / Rejected / Withdrawn) is preserved in accepted_values so
      future runs that include in-flight applications pass without a
      schema change.
    - Q1 2020 hires (12 employees: Maya, David, Priya, plus 9 founders)
      are excluded by design -- the company's spec narrative places
      Ashby adoption at Q2 2020 onward.
    - Stage-dependent nullability (per spec):
        phone_screen_date  populated only if candidate reached Phone
                           Screen or beyond
        onsite_date        populated only if reached Onsite or beyond
        offer_date         populated only if Offer was extended
        hire_date          populated only if current_stage = Hired
        offer_accepted     TRUE on Hired rows; FALSE on Offer-stage
                           Withdrawn rows; null otherwise
        rejection_reason   populated on Rejected / Withdrawn rows; null
                           on Hired rows
    - Section 12 HR17 enforces (hire_date, department, candidate_name)
      uniqueness on Hired rows against stg_employees; HR18 enforces
      application_date < hire_date when hire_date is populated.
*/



with source as (
    select * from `just-kaizen-ai`.`raw_raw`.`raw_recruiting`
),

renamed as (
    select
        -- Identifiers
        application_id,
        requisition_id,

        -- Candidate
        candidate_name,

        -- Requisition placement (matches the at-hire state of the hired
        -- candidate, NOT the candidate's own placement)
        job_title,
        department,
        sub_department,
        recruiter,
        hiring_manager,
        source,

        -- Funnel state
        current_stage,

        -- Stage timestamps (cumulative; populated only if reached)
        application_date,
        phone_screen_date,
        onsite_date,
        offer_date,
        hire_date,

        -- Outcome
        offer_accepted,
        rejection_reason
    from source
)

select * from renamed;

