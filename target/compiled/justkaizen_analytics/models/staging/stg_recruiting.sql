/*
    Model:  stg_recruiting
    Layer:  Staging
    Source: raw.raw_offers_hires (ATS export)
    Grain:  One row per candidate-job
    PK:     candidate_id

    Purpose:
        Rename verbose ATS field names to clean snake_case. Compute
        time_to_fill_days and time_to_hire_days. Resolve employee_id for
        hired candidates by joining back to raw_employees on Requisition_ID.
        Rename Source -> application_channel ("source" is reserved in
        several SQL contexts and is renamed for safety).

    Note on the join:
        Strict staging convention is "no joins". This model deviates per
        the data dictionary's explicit instruction to surface employee_id
        for hired candidates so the roster can attach
        candidate_source / candidate_origin / candidate_recruiter /
        candidate_hiring_manager without re-deriving the link. The lookup
        is a one-to-one read against a deduplicated subset of raw_employees
        and contains no business logic.

    Derived fields (per spec):
        - offer_accepted: outcome = 'Hired' AND offer_accept_date IS NOT NULL
        - time_to_fill_days: start_date - requisition_fill_start_date
        - time_to_hire_days: start_date - application_date

    Date casting:
        All date columns arrive from the CSV seed as STRING in MM/DD/YYYY
        format. SAFE.PARSE_DATE wraps every date field so downstream models
        (the roster, recruiting mart) receive proper DATE types and
        DATE_DIFF works without inline re-parsing.
*/

with source as (

    select * from `just-kaizen-ai`.`raw_raw`.`raw_offers_hires`

),

employees_for_link as (

    -- One employee_id per Requisition_ID. Synthetic data is expected to
    -- maintain a 1:1 relationship between requisition and hire; DISTINCT
    -- guards against duplicate Report_Date snapshot rows in raw_employees.
    select distinct
        Requisition_ID                                                      as requisition_id,
        Work_Email                                                          as employee_id
    from `just-kaizen-ai`.`raw_raw`.`raw_employees`
    where Work_Email is not null

),

renamed as (

    select
        s.Candidate_ID                                                      as candidate_id,
        s.Requisition_ID                                                    as requisition_id,

        -- employee_id only meaningful for hired candidates
        case when s.Outcome = 'Hired' then e.employee_id end                as employee_id,

        safe.parse_date('%m/%d/%Y', cast(s.Requisition_Fill_Start_Date as string))
                                                                            as requisition_fill_start_date,
        s.Outcome                                                           as outcome,
        s.Job                                                               as job_title,
        s.Job_Status                                                        as job_status,
        s.Recruiter                                                         as recruiter,
        s.Hiring_Manager                                                    as hiring_manager,
        s.Origin                                                            as origin,
        s.Source                                                            as application_channel,

        s.Current_Interview_Stage                                           as current_interview_stage,
        s.Furthest_Stage_Reached                                            as furthest_stage_reached,
        s.Archive_Reason                                                    as archive_reason,
        s.Offer_Decline_Category                                            as offer_decline_category,

        safe.parse_date('%m/%d/%Y', cast(s.Candidate_Application_Date as string))
                                                                            as application_date,
        safe.parse_date('%m/%d/%Y', cast(s.Candidate_Stage_1_Interview_Date as string))
                                                                            as stage_1_interview_date,
        safe.parse_date('%m/%d/%Y', cast(s.Candidate_Stage_2_Interview_Date as string))
                                                                            as stage_2_interview_date,
        safe.parse_date('%m/%d/%Y', cast(s.Candidate_Offer_Stage_Entered_Date as string))
                                                                            as offer_extended_date,
        safe.parse_date('%m/%d/%Y', cast(s.Candidate_Offer_Accept_Date as string))
                                                                            as offer_accept_date,
        safe.parse_date('%m/%d/%Y', cast(s.Candidate_Start_Date as string))
                                                                            as start_date,

        -- Derived flags & durations (parse dates inline so DATE_DIFF
        -- receives DATE arguments regardless of seed casting).
        (s.Outcome = 'Hired' and s.Candidate_Offer_Accept_Date is not null) as offer_accepted,
        date_diff(
            safe.parse_date('%m/%d/%Y', cast(s.Candidate_Start_Date as string)),
            safe.parse_date('%m/%d/%Y', cast(s.Requisition_Fill_Start_Date as string)),
            day
        )                                                                   as time_to_fill_days,
        date_diff(
            safe.parse_date('%m/%d/%Y', cast(s.Candidate_Start_Date as string)),
            safe.parse_date('%m/%d/%Y', cast(s.Candidate_Application_Date as string)),
            day
        )                                                                   as time_to_hire_days

    from source as s
    left join employees_for_link as e
        on s.Requisition_ID = e.requisition_id

),

final as (

    select * from renamed

)

select * from final