
    
    

with all_values as (

    select
        job_level as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_employees`
    group by job_level

)

select *
from all_values
where value_field not in (
    'IC1','IC2','IC3','IC4','IC5','M1','M2','M3','M4','M5'
)


