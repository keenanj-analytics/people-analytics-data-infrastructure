
    
    

with dbt_test__target as (

  select application_id as unique_field
  from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
  where application_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


