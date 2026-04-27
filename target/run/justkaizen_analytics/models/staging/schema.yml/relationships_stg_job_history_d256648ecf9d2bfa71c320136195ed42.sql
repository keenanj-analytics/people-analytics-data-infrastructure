
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select new_manager_id as from_field
    from (select * from `just-kaizen-ai`.`raw_staging`.`stg_job_history` where new_manager_id is not null) dbt_subquery
    where new_manager_id is not null
),

parent as (
    select employee_id as to_field
    from `just-kaizen-ai`.`raw_staging`.`stg_employees`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test