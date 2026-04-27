
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        change_reason as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_compensation`
    group by change_reason

)

select *
from all_values
where value_field not in (
    'New Hire','Annual Review','Promotion','Market Adjustment','Equity Adjustment'
)



  
  
      
    ) dbt_internal_test