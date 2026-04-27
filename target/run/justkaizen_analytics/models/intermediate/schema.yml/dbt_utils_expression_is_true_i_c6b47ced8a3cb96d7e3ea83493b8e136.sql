
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  



select
    1
from (select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_tenure` where months_since_last_promotion is not null) dbt_subquery

where not(months_since_last_promotion >= 0)


  
  
      
    ) dbt_internal_test