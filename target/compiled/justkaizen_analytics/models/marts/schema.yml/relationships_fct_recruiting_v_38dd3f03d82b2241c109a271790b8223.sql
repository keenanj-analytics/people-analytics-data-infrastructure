
    
    

with child as (
    select requisition_id as from_field
    from `just-kaizen-ai`.`raw_marts`.`fct_recruiting_velocity`
    where requisition_id is not null
),

parent as (
    select requisition_id as to_field
    from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


