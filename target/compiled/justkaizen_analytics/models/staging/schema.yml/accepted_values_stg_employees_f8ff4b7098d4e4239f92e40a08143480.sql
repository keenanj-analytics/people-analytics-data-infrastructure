
    
    

with all_values as (

    select
        termination_type as value_field,
        count(*) as n_records

    from (select * from `just-kaizen-ai`.`raw_staging`.`stg_employees` where termination_type is not null) dbt_subquery
    group by termination_type

)

select *
from all_values
where value_field not in (
    'Voluntary','Involuntary','Layoff'
)


