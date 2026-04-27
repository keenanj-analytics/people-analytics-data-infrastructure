
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        review_status as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_performance`
    group by review_status

)

select *
from all_values
where value_field not in (
    'Completed','Incomplete','Exempt'
)



  
  
      
    ) dbt_internal_test