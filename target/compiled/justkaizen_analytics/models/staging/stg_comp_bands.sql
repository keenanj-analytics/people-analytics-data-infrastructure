/*
    Model:  stg_comp_bands
    Layer:  Staging
    Source: raw.raw_comp_bands (Total Rewards export)
    Grain:  One row per job title
    PK:     job_title

    Purpose:
        Rename to snake_case. Parse salary strings ("$174,000") to FLOAT64.
        Title -> job_title for clean joins back to stg_employees.

    Salary parse:
        Strip "$" and "," before SAFE_CAST AS FLOAT64. The CAST AS STRING
        wrapper makes REPLACE safe against already-numeric input from
        seeds that infer types.
*/

with source as (

    select * from `just-kaizen-ai`.`raw_raw`.`raw_comp_bands`

),

renamed as (

    select
        Title                                                               as job_title,
        Department                                                          as department,
        Job_Family                                                          as job_family,
        Job_Code                                                            as job_code,
        Level                                                               as job_level,

        safe_cast(replace(replace(cast(Zone_A_Min_Salary as string), '$', ''), ',', '') as float64)
                                                                            as zone_a_min_salary,
        safe_cast(replace(replace(cast(Zone_A_Mid_Salary as string), '$', ''), ',', '') as float64)
                                                                            as zone_a_mid_salary,
        safe_cast(replace(replace(cast(Zone_A_Max_Salary as string), '$', ''), ',', '') as float64)
                                                                            as zone_a_max_salary,
        safe_cast(replace(replace(cast(Zone_B_Min_Salary as string), '$', ''), ',', '') as float64)
                                                                            as zone_b_min_salary,
        safe_cast(replace(replace(cast(Zone_B_Mid_Salary as string), '$', ''), ',', '') as float64)
                                                                            as zone_b_mid_salary,
        safe_cast(replace(replace(cast(Zone_B_Max_Salary as string), '$', ''), ',', '') as float64)
                                                                            as zone_b_max_salary

    from source

),

final as (

    select * from renamed

)

select * from final