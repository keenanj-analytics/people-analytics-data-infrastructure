
    
    

with child as (
    select manager_id as from_field
    from (select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension` where manager_id is not null) dbt_subquery
    where manager_id is not null
),

parent as (
    select employee_id as to_field
    from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


