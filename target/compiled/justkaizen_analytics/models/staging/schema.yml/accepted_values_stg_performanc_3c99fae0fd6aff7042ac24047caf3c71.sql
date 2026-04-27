
    
    

with all_values as (

    select
        self_rating as value_field,
        count(*) as n_records

    from (select * from `just-kaizen-ai`.`raw_staging`.`stg_performance` where self_rating is not null) dbt_subquery
    group by self_rating

)

select *
from all_values
where value_field not in (
    'Significantly Exceeds','Exceeds','Meets','Partially Meets','Does Not Meet'
)


