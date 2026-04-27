
    
    

with all_values as (

    select
        change_type as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_job_history`
    group by change_type

)

select *
from all_values
where value_field not in (
    'Hire','Promotion','Lateral Transfer','Department Transfer','Title Change','Manager Change'
)


