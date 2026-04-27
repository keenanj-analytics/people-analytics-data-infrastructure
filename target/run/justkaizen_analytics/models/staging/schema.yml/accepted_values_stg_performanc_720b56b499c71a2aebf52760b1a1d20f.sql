
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        manager_rating as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_performance`
    group by manager_rating

)

select *
from all_values
where value_field not in (
    'Significantly Exceeds','Exceeds','Meets','Partially Meets','Does Not Meet'
)



  
  
      
    ) dbt_internal_test