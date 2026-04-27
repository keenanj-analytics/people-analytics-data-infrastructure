
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select cohort_size
from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`
where cohort_size is null



  
  
      
    ) dbt_internal_test