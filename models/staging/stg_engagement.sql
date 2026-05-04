/*
    Model:  stg_engagement
    Layer:  Staging
    Source: raw.raw_ees_responses (engagement survey export)
    Grain:  One row per (anonymized_user_id, ees_question, ees_cycle)

    Purpose:
        Clean field names to snake_case. Rename Radford_Level -> job_level
        for warehouse-wide consistency. Cast EES_Submission_Date to DATE.

    Notes:
        - Data is anonymized. There is no employee_id link to the roster;
          downstream uses aggregate (theme x department x cycle) before
          reporting.
        - eNPS (0-10) and Response_Likert (1-5) are mutually exclusive on a
          row — only one is populated based on question type.
*/

with source as (

    select * from {{ source('raw', 'raw_ees_responses') }}

),

renamed as (

    select
        Anonymized_User_ID                                                  as anonymized_user_id,
        EES_Cycle                                                           as ees_cycle,
        safe.parse_date('%m/%d/%Y', cast(EES_Submission_Date as string))    as ees_submission_date,
        EES_Theme_Name                                                      as ees_theme_name,
        EES_Question                                                        as ees_question,

        eNPS                                                                as enps,
        enps_Category                                                       as enps_category,
        Response_Likert                                                     as response_likert,

        Department                                                          as department,
        Sub_Department                                                      as sub_department,
        Team                                                                as team,
        Tenure_Bucket                                                       as tenure_bucket,
        Radford_Level                                                       as job_level,
        Is_A_Manager                                                        as is_a_manager,
        Is_Top_Performer                                                    as is_top_performer

    from source

),

final as (

    select * from renamed

)

select * from final
