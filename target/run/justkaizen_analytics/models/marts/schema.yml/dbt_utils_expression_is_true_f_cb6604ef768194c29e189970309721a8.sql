
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`

where not(cohort_size >= 1)


  
  
      
    ) dbt_internal_test