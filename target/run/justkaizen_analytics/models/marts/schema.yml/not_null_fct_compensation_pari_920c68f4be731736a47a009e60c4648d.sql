
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select salary_percentile_within_dept_level
from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`
where salary_percentile_within_dept_level is null



  
  
      
    ) dbt_internal_test