/*
    Analysis:  Headcount growth with layoff overlay
    Mart:      fct_workforce_overview
    Audience:  CEO / Board, quarterly review

    Question
    --------
    "Show me net headcount change quarter by quarter from 2020 through
    2025-Q1, broken down by hire vs termination flow. Highlight the
    Q1 2023 layoff against the surrounding growth narrative."

    Result shape
    ------------
    21 rows, one per quarter, with columns the BI tool can render as a
    stacked bar (hires above zero, terminations below zero, net change
    as a line overlay).

    Pattern
    -------
    Aggregates fct_workforce_overview across all 7 departments, then
    splits terminations by type so the Q1 2023 layoff bar visually
    distinguishes itself from the surrounding voluntary attrition.
*/

select
    year_quarter,
    quarter_end_date,

    -- Aggregate flows across all departments
    sum(hires_in_period)                    as company_hires,
    sum(voluntary_terminations)             as voluntary_attrition,
    sum(involuntary_terminations)           as performance_terminations,
    sum(layoff_terminations)                as layoffs,
    sum(terminations_in_period)             as total_terminations,
    sum(hires_in_period) - sum(terminations_in_period)  as net_change,

    -- Headcount snapshot
    sum(end_headcount)                      as company_end_headcount,

    -- Surface the layoff quarter for easy filtering / formatting
    case
        when sum(layoff_terminations) > 0 then 'Layoff Quarter'
        when sum(hires_in_period) = 0 and sum(terminations_in_period) > 0 then 'Hiring Freeze'
        when sum(hires_in_period) > 60 then 'Hypergrowth'
        else 'Normal'
    end as period_classification
from {{ ref('fct_workforce_overview') }}
group by year_quarter, quarter_end_date
order by quarter_end_date
