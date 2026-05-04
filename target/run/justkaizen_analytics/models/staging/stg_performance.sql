

  create or replace view `just-kaizen-ai`.`raw_staging`.`stg_performance`
  OPTIONS()
  as /*
    Model:  stg_performance
    Layer:  Staging
    Source: raw.raw_performance (Lattice export)
    Grain:  One row per employee per review cycle (post-filter)
    PK:     (employee_id, cycle_name)

    Purpose:
        Filter to the official manager rating row, then invert Lattice's
        1-best 4-point scale to JustKaizen's 5-best 5-point scale. Clean
        field names. Output one rating per (employee, cycle).

    Critical filter (collapses the source grain):
        Response_Type = 'manager' AND Question = 'Performance Category'
        — drops self reviews, peer reviews, and free-text question rows.

    Rating inversion (per data dictionary, plus JustKaizen's source-5
    extension for "Partially Meets"):
        Source 1 ("Truly Outstanding")             -> 5 ("Significantly Exceeds Expectations")
        Source 2 ("Frequently Exceeds")            -> 4 ("Exceeds Expectations")
        Source 3 ("Strong Contributor")            -> 3 ("Meets Expectations")
        Source 4 ("Does Not Meet Expectations")    -> 1 ("Does Not Meet Expectations")
        Source 5 ("Partially Meets Expectations")  -> 2 ("Partially Meets Expectations")

    Notes:
        - Lattice's native scale is 1-4. Source 5 is a JustKaizen extension
          emitted by the synthetic generator for the "Partially Meets"
          target rating, which doesn't exist in real Lattice exports.
        - Calibrated_Score is intentionally dropped; the warehouse uses
          the raw Score per the spec.
*/

with source as (

    select * from `just-kaizen-ai`.`raw_raw`.`raw_performance`

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

        -- Inverted numeric rating (source 1=best -> target 5=best;
        -- source 5 is the JustKaizen "Partially Meets" extension).
        case safe_cast(Score as int64)
            when 1 then 5
            when 2 then 4
            when 3 then 3
            when 4 then 1
            when 5 then 2
        end                                                                 as overall_rating_numeric,

        -- Cleaned target description
        case Score_Description
            when '1 - Truly Outstanding'                  then 'Significantly Exceeds Expectations'
            when '2 - Frequently Exceeds Expectations'    then 'Exceeds Expectations'
            when '3 - Strong Contributor'                 then 'Meets Expectations'
            when '4 - Does Not Meet Expectations'         then 'Does Not Meet Expectations'
            when '5 - Partially Meets Expectations'       then 'Partially Meets Expectations'
        end                                                                 as overall_rating,

        Response_Text                                                       as response_text

    from filtered

),

final as (

    select * from renamed

)

select * from final;

