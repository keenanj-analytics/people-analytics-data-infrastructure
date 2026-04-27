
    
    

with all_values as (

    select
        department as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_engagement`
    group by department

)

select *
from all_values
where value_field not in (
    'Engineering','Sales','Customer Success','Marketing','Product','G&A','People'
)


