
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select compa_ratio
from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`
where compa_ratio is null



  
  
      
    ) dbt_internal_test