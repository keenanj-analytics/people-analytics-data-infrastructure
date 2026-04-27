



select
    1
from (select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_event_sequence` where days_since_prev_event is not null) dbt_subquery

where not(days_since_prev_event >= 0)

