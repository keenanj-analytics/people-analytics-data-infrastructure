
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        application_channel as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_recruiting`
    group by application_channel

)

select *
from all_values
where value_field not in (
    'Referral','LinkedIn','Job Board','Agency','Internal','Career Page','Event'
)



  
  
      
    ) dbt_internal_test