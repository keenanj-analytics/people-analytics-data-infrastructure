
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        year_quarter, department
    from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`
    group by year_quarter, department
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test