
    
    

with all_values as (

    select
        survey_cycle as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_engagement`
    group by survey_cycle

)

select *
from all_values
where value_field not in (
    '2021-Q2','2021-Q3','2021-Q4','2022-Q1','2022-Q2','2022-Q3','2022-Q4','2023-Q1','2023-Q2','2023-Q3','2023-Q4','2024-Q1','2024-Q2','2024-Q3','2024-Q4','2025-Q1'
)


