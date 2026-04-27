
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select job_level
from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`
where job_level is null



  
  
      
    ) dbt_internal_test