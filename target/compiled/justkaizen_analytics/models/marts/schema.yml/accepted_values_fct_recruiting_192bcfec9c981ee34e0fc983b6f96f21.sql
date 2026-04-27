
    
    

with all_values as (

    select
        top_source_for_requisition as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`
    group by top_source_for_requisition

)

select *
from all_values
where value_field not in (
    'Referral','LinkedIn','Job Board','Agency','Internal','Career Page','Event'
)


