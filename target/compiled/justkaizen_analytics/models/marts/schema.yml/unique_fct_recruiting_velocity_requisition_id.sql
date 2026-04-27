
    
    

with dbt_test__target as (

  select requisition_id as unique_field
  from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`
  where requisition_id is not null

)

select
    unique_field,
    count(*) as n_records

from dbt_test__target
group by unique_field
having count(*) > 1


