
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select quarter_end_date
from `just-kaizen-ai`.`raw_marts`.`fct_workforce_overview`
where quarter_end_date is null



  
  
      
    ) dbt_internal_test