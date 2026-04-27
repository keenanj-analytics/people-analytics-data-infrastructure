
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        rating_distribution_band as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
    group by rating_distribution_band

)

select *
from all_values
where value_field not in (
    'High','On Target','Below'
)



  
  
      
    ) dbt_internal_test