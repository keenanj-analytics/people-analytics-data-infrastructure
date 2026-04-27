
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select is_top_performer
from `just-kaizen-ai`.`raw_marts`.`fct_performance_distribution`
where is_top_performer is null



  
  
      
    ) dbt_internal_test