
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cycle_end_date
from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`
where cycle_end_date is null



  
  
      
    ) dbt_internal_test