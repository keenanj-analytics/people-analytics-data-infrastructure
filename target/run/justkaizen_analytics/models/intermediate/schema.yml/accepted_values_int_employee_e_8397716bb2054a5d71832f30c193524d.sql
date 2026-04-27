
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        change_type as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_intermediate`.`int_employee_event_sequence`
    group by change_type

)

select *
from all_values
where value_field not in (
    'Hire','Promotion','Lateral Transfer','Department Transfer','Title Change','Manager Change'
)



  
  
      
    ) dbt_internal_test