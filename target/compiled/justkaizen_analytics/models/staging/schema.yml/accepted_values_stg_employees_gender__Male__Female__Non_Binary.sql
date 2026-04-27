
    
    

with all_values as (

    select
        gender as value_field,
        count(*) as n_records

    from (select * from `just-kaizen-ai`.`raw_staging`.`stg_employees` where gender is not null) dbt_subquery
    group by gender

)

select *
from all_values
where value_field not in (
    'Male','Female','Non-Binary'
)


