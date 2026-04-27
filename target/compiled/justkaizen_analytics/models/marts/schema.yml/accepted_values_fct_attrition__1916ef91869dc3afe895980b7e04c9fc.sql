
    
    

with all_values as (

    select
        termination_type as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_marts`.`fct_attrition_drivers`
    group by termination_type

)

select *
from all_values
where value_field not in (
    'Voluntary','Involuntary','Layoff'
)


