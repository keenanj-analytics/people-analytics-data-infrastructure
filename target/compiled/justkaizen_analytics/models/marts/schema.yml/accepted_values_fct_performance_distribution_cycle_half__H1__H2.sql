
    
    

with all_values as (

    select
        cycle_half as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
    group by cycle_half

)

select *
from all_values
where value_field not in (
    'H1','H2'
)


