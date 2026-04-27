
    
    

with all_values as (

    select
        overall_rating as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_performance`
    group by overall_rating

)

select *
from all_values
where value_field not in (
    'Significantly Exceeds','Exceeds','Meets','Partially Meets','Does Not Meet'
)


