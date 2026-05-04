/*
    Model:        fct_employee_roster
    Layer:        Mart — drill-through
    Materialized: table (mart-layer default)
    Grain:        One row per (employee_id, report_month)
                  Same as int_employee_monthly_roster
    Source:       int_employee_monthly_roster (direct promotion)

    Purpose:
        The drill-through table Tableau filters when the CHRO asks
        "who left?" or "who is below band?". Promotes the roster view
        to a table for fast filtering, plus two analyst-friendly
        derived columns on top of the existing 46.

    Added columns:
        - is_active: boolean form of employment_status, easier to
          filter on in Tableau than the string equivalent.
        - band_position_label: bucketed compa_ratio for "Below Band /
          Within Band / Above Band" — the most common compensation
          drill-through filter.

    Notes:
        - All 46 columns from int_employee_monthly_roster pass through
          unchanged. full_name already lives on the roster as the ADP-
          provided field (no CONCAT needed) — kept as-is.
        - band_position_label returns NULL for rows without a
          compa_ratio (employees missing a comp band match), rather
          than forcing them into one of the three buckets.
*/

with roster as (

    select * from `just-kaizen-ai`.`raw_intermediate`.`int_employee_monthly_roster`

),

final as (

    select
        roster.*,
        employment_status = 'Active'                            as is_active,
        case
            when compa_ratio is null    then null
            when compa_ratio < 0.90     then 'Below Band'
            when compa_ratio > 1.10     then 'Above Band'
            else                             'Within Band'
        end                                                     as band_position_label
    from roster

)

select * from final