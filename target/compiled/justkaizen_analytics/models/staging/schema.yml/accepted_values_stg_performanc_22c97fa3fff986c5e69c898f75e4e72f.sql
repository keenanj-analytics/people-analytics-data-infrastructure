
    
    

with all_values as (

    select
        review_cycle as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_performance`
    group by review_cycle

)

select *
from all_values
where value_field not in (
    '2020-H2','2021-H1','2021-H2','2022-H1','2022-H2','2023-H1','2023-H2','2024-H1','2024-H2'
)


