
    
    

with all_values as (

    select
        race_ethnicity as value_field,
        count(*) as n_records

    from (select * from `just-kaizen-ai`.`raw_staging`.`stg_employees` where race_ethnicity is not null) dbt_subquery
    group by race_ethnicity

)

select *
from all_values
where value_field not in (
    'White','Asian','Hispanic/Latino','Black','Two or More','Other/Decline'
)


