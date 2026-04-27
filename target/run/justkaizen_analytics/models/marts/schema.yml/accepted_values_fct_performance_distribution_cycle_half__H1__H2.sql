
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        cycle_half as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
    group by cycle_half

)

select *
from all_values
where value_field not in (
    'H1','H2'
)



  
  
      
    ) dbt_internal_test