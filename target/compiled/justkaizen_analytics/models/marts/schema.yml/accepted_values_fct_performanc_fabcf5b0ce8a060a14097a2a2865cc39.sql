
    
    

with all_values as (

    select
        rating_distribution_band as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
    group by rating_distribution_band

)

select *
from all_values
where value_field not in (
    'High','On Target','Below'
)


