
    
    

with all_values as (

    select
        top_application_channel as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
    group by top_application_channel

)

select *
from all_values
where value_field not in (
    'Referral','LinkedIn','Job Board','Agency','Internal','Career Page','Event'
)


