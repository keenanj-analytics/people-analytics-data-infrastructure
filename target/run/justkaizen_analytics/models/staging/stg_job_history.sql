

  create or replace view `just-kaizen-ai`.`raw_staging`.`stg_job_history`
  OPTIONS()
  as /*
    Model:  stg_job_history
    Layer:  Staging
    Source: raw.raw_job_history (job change events)
    Grain:  One row per (employee_id, effective_date, change_type)

    Purpose:
        Clean rename. Cast effective_date to DATE. No joins, no business
        logic. Field names follow the architecture spec's old_X / new_X
        pair convention.

    Notes:
        - The data dictionary flags raw_job_history as "(if separate)";
          job history may instead be derived from monthly raw_employees
          snapshots in the rescale phase. If that path is chosen, this
          model and its sources.yml entry should be dropped.
        - The new_X source columns stay backtick-quoted because BigQuery
          rejects bare new_X identifiers in a select list (NEW is a
          reserved keyword and the parser is conservative about prefixed
          identifiers).
*/

with source as (

    select * from `just-kaizen-ai`.`raw_raw`.`raw_job_history`

),

renamed as (

    select
        employee_id                                                         as employee_id,
        safe.parse_date('%m/%d/%Y', cast(effective_date as string))         as effective_date,
        change_type                                                         as change_type,

        old_department                                                      as old_department,
        `new_department`                                                    as new_department,
        old_sub_department                                                  as old_sub_department,
        `new_sub_department`                                                as new_sub_department,
        old_job_level                                                       as old_job_level,
        `new_job_level`                                                     as new_job_level,
        old_job_title                                                       as old_job_title,
        `new_job_title`                                                     as new_job_title,
        old_manager_id                                                      as old_manager_id,
        `new_manager_id`                                                    as new_manager_id

    from source

),

final as (

    select * from renamed

)

select * from final;

