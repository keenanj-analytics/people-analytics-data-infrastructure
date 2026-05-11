/*
    Model:  stg_performance
    Layer:  Staging
    Source: raw.raw_performance (performance management system export)
    Grain:  One row per employee per review cycle (post-filter)
    PK:     (employee_id, cycle_name)

    Purpose:
        Filter to the official manager rating row, then invert the source
        system's 1-best 4-point scale to JustKaizen's 5-best 5-point
        scale. Clean field names. Output one rating per (employee, cycle).

    Critical filter (collapses the source grain):
        Response_Type = 'manager' AND Question = 'Performance Category'
        — drops self reviews, peer reviews, and free-text question rows.

    Rating inversion (source 1=best -> target 5=best):
        Source 1 ("Outstanding")                   -> 5 ("Significantly Exceeds Expectations")
        Source 2 ("Exceeds Expectations")          -> 4 ("Exceeds Expectations")
        Source 3 ("Strong Contributor")            -> 3 ("Meets Expectations")
        Source 4 ("Partially Meets Expectations")  -> 2 ("Partially Meets Expectations")
        Source 5 ("Does Not Meet Expectations")    -> 1 ("Does Not Meet Expectations")

    Notes:
        - Source uses 1-5 scale where 1 is best. Target uses 1-5 scale
          where 5 is best.
        - Calibrated_Score is intentionally dropped; the warehouse uses
          the raw Score per the spec.
*/

with source as (

    select * from {{ source('raw', 'raw_performance') }}

),

filtered as (

    select *
    from source
    where Response_Type = 'manager'
      and Question = 'Performance Category'

),

renamed as (

    select
        Reviewee_Email                                                      as employee_id,
        Reviewee_Name                                                       as reviewee_name,
        Cycle_Name                                                          as cycle_name,
        Reviewer_Name                                                       as reviewer_name,
        Reviewer_email                                                      as reviewer_email,

        -- Inverted numeric rating (source 1=best -> target 5=best).
        case safe_cast(Score as int64)
            when 1 then 5
            when 2 then 4
            when 3 then 3
            when 4 then 2
            when 5 then 1
        end                                                                 as overall_rating_numeric,

        -- Cleaned target description
        case Score_Description
            when '1 - Outstanding'                        then 'Significantly Exceeds Expectations'
            when '2 - Exceeds Expectations'               then 'Exceeds Expectations'
            when '3 - Strong Contributor'                 then 'Meets Expectations'
            when '4 - Partially Meets Expectations'       then 'Partially Meets Expectations'
            when '5 - Does Not Meet Expectations'         then 'Does Not Meet Expectations'
        end                                                                 as overall_rating,

        Response_Text                                                       as response_text

    from filtered

),

final as (

    select * from renamed

)

select * from final
