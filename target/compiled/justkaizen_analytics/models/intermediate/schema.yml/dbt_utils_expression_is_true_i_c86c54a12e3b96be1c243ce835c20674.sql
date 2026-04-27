



select
    1
from (select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_performance_history` where self_optimism_delta is not null) dbt_subquery

where not(self_optimism_delta between -4 and 4)

