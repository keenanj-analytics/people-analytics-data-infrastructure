
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select compa_ratio_quartile_label
from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`
where compa_ratio_quartile_label is null



  
  
      
    ) dbt_internal_test