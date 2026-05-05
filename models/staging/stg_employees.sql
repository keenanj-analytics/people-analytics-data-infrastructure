/*
    Model:  stg_employees
    Layer:  Staging
    Source: raw.raw_employees (HRIS export)
    Grain:  One row per employee per Report_Date snapshot
    PK:     work_email (= employee_id)

    Purpose:
        Clean field names, cast string dates to DATE, surface employee_id
        and manager_id as canonical identifier columns. No joins, no
        aggregation, no derived business logic — rename, cast, pass through.

    Notes:
        - employee_id and manager_id are both work emails. raw_employees
          uses email as the natural PK; manager_id is renamed from
          Manager_Email and joins back to employee_id downstream.
        - Hire_Date and Termination_Date arrive as MM/DD/YYYY strings per
          the data dictionary; SAFE.PARSE_DATE so malformed rows land as
          NULL rather than fail the build.
        - Pay_Zone is renamed to employee_zone per the data dictionary.
*/

with source as (

    select * from {{ source('raw', 'raw_employees') }}

),

renamed as (

    select
        -- Identifiers
        Work_Email                                                          as employee_id,
        Work_Email                                                          as work_email,
        Position_ID                                                         as position_id,
        Requisition_ID                                                      as requisition_id,
        Manager_Email                                                       as manager_id,
        Manage_Name                                                         as manager_name,

        -- Snapshot (string -> DATE so downstream ORDER BY and date
        -- arithmetic work chronologically, not alphabetically)
        safe.parse_date('%m/%d/%Y', cast(Report_Date as string))            as report_date,
        Employment_Status                                                   as employment_status,

        -- Names
        Full_Name                                                           as full_name,
        First_Name                                                          as first_name,
        Last_Name                                                           as last_name,

        -- Dates (string -> DATE)
        safe.parse_date('%m/%d/%Y', cast(Hire_Date as string))              as hire_date,
        safe.parse_date('%m/%d/%Y', cast(Termination_Date as string))       as termination_date,

        -- Location
        Work_Country                                                        as work_country,
        Work_City                                                           as work_city,
        Work_State                                                          as work_state,
        Pay_Zone                                                            as employee_zone,

        -- Demographics
        Gender                                                              as gender,
        Race                                                                as race,

        -- Org
        Department                                                          as department,
        Sub_Department                                                      as sub_department,
        Team                                                                as team,
        Job_Title                                                           as job_title,
        Job_Level                                                           as job_level,
        Employment_Type                                                     as employment_type,

        -- Compensation (passthrough; raw_employees gains a Salary column
        -- during the rescale phase, joined to comp bands by
        -- int_employee_compensation_current via job_title + employee_zone)
        Salary                                                              as salary,

        -- Termination detail
        Termination_Reason                                                  as termination_reason,
        Termination_Regrettable                                             as termination_regrettable,
        Termination_Voluntary                                               as termination_voluntary,

        -- Management
        No_Direct_Reports                                                   as no_direct_reports,
        No_Indirect_Reports                                                 as no_indirect_reports,
        Manager_Status                                                      as manager_status,

        -- Tenure & flags (tenure_bucket recomputed on the roster with
        -- 1-year intervals; passed through here as-is from HRIS)
        Tenure_bucket                                                       as tenure_bucket,
        Critical_Talent                                                     as critical_talent,
        Hire_Origin                                                         as hire_origin,
        Hire_Recruiter                                                      as hire_recruiter

    from source

),

final as (

    select * from renamed

)

select * from final
