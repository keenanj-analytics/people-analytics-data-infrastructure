
    
    

with all_values as (

    select
        current_comp_change_reason as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_intermediate`.`int_employee_compensation_current`
    group by current_comp_change_reason

)

select *
from all_values
where value_field not in (
    'New Hire','Annual Review','Promotion','Market Adjustment','Equity Adjustment'
)


