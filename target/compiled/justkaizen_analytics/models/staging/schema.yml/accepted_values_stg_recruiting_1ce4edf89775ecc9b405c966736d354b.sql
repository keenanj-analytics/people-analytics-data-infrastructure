
    
    

with all_values as (

    select
        current_stage as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
    group by current_stage

)

select *
from all_values
where value_field not in (
    'Applied','Phone Screen','Technical','Onsite','Offer','Hired','Rejected','Withdrawn'
)


