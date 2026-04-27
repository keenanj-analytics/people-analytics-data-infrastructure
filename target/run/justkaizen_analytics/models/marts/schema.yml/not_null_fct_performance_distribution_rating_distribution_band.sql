
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select rating_distribution_band
from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
where rating_distribution_band is null



  
  
      
    ) dbt_internal_test