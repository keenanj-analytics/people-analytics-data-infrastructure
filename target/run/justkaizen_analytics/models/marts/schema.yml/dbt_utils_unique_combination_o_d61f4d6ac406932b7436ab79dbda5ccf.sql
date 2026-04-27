
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        survey_cycle, department, theme
    from `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
    group by survey_cycle, department, theme
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test