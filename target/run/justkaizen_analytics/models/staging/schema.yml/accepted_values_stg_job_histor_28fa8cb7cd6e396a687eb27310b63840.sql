
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        new_job_level as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_job_history`
    group by new_job_level

)

select *
from all_values
where value_field not in (
    'IC1','IC2','IC3','IC4','IC5','M1','M2','M3','M4','M5'
)



  
  
      
    ) dbt_internal_test