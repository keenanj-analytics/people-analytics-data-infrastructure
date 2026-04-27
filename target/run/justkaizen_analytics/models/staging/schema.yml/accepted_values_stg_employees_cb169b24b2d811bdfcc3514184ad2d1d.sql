
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        race_ethnicity as value_field,
        count(*) as n_records

    from (select * from `just-kaizen-ai`.`raw_staging`.`stg_employees` where race_ethnicity is not null) dbt_subquery
    group by race_ethnicity

)

select *
from all_values
where value_field not in (
    'White','Asian','Hispanic/Latino','Black','Two or More','Other/Decline'
)



  
  
      
    ) dbt_internal_test