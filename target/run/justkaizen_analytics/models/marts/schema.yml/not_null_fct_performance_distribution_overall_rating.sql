
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select overall_rating
from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
where overall_rating is null



  
  
      
    ) dbt_internal_test