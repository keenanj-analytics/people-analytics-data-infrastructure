
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        compa_ratio_quartile_label as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`
    group by compa_ratio_quartile_label

)

select *
from all_values
where value_field not in (
    'Top Quartile','Q3','Q2','Bottom Quartile'
)



  
  
      
    ) dbt_internal_test