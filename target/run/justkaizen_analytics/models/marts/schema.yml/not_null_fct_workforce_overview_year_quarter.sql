
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select year_quarter
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`
where year_quarter is null



  
  
      
    ) dbt_internal_test