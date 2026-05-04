/*
    Model:        dim_calendar
    Layer:        Intermediate
    Materialized: table
    Grain:        One row per calendar day
    Range:        2020-01-01 through 2026-12-31 (~2,557 rows)

    Purpose:
        Daily date spine. Anchors every month-grain model in the warehouse.
        Reporting grids CROSS JOIN distinct report_months from this table
        against dimension combos from int_employee_monthly_roster, which
        guarantees a row in every trend line for every month — even months
        with zero activity. TTM rolling windows in the marts read their
        report_month sequence from this spine.

    Sources:
        None. Generated entirely via GENERATE_DATE_ARRAY so this model
        has no upstream dependencies and can build as step 1.

    Key business rules:
        - report_quarter formatted as "YYYY Q#" (e.g., "2025 Q1") to match
          the format carried through every domain reporting mart.
        - is_quarter_end is TRUE only on the final calendar day of months
          3, 6, 9, 12 — used by Tableau quarter-end flags.
        - flag_latest_month marks the most recent complete calendar month
          relative to CURRENT_DATE(). This is a self-contained proxy for
          "MAX(report_month) with data" — chosen so dim_calendar has no
          upstream dependencies per the build order. Once
          int_employee_monthly_roster exists, this column can be refactored
          to reference MAX(report_month) from the roster for precision.

    Note:
        Materialization (table) is set in dbt_project.yml as a per-model
        override of the intermediate-layer default (view).
*/

with date_spine as (

    select calendar_date
    from unnest(
        generate_date_array(date '2020-01-01', date '2026-12-31', interval 1 day)
    ) as calendar_date

),

final as (

    select
        calendar_date,
        date_trunc(calendar_date, month) as report_month,
        concat(
            cast(extract(year from calendar_date) as string),
            ' Q',
            cast(extract(quarter from calendar_date) as string)
        ) as report_quarter,
        extract(year from calendar_date) as report_year,
        calendar_date = last_day(calendar_date, month) as is_month_end,
        (
            calendar_date = last_day(calendar_date, month)
            and extract(month from calendar_date) in (3, 6, 9, 12)
        ) as is_quarter_end,
        date_trunc(calendar_date, month)
            = date_sub(date_trunc(current_date(), month), interval 1 month)
            as flag_latest_month
    from date_spine

)

select * from final