
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select total_promotions
from `just-kaizen-ai`.`raw_intermediate`.`int_employee_dimension`
where total_promotions is null



  
  
      
    ) dbt_internal_test