
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        question_id as value_field,
        count(*) as n_records

    from `just-kaizen-ai`.`raw_staging`.`stg_engagement`
    group by question_id

)

select *
from all_values
where value_field not in (
    'ENG1','ENG2','ENG3','ENG4','MGE1','MGE2','MGE3','MGE4','MGE5','CGD1','CGD2','CGD3','WLB1','WLB2','WLB3','REC1','REC2','REC3','CUL1','CUL2','CUL3','COM1','COM2','COM3','RES1','RES2','RES3'
)



  
  
      
    ) dbt_internal_test