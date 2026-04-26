/*
    Analysis:  Manager Change as an attrition leading indicator
    Mart:      fct_attrition_drivers
    Audience:  People Ops, manager-effectiveness review

    Question
    --------
    "Of the people who left voluntarily, what % had a manager change
    in the 6 months before their departure? Is that share materially
    higher than the all-voluntary baseline? Break down by department
    so we can see where this pattern is concentrated."

    What we expect to find
    ----------------------
    The Manager Change Casualty archetype (20 profiles) is defined by
    having a manager change 3-9 months before departure with a
    Manager Relationship / Company Culture / Career Opportunity
    termination reason. So these 20 are guaranteed to flag
    `terminated_within_6mo_of_manager_change = TRUE`.

    Among the broader voluntary-termination pool, the rate of recent
    manager changes is higher than would be expected from random
    timing alone -- which is the analytical signal: a manager change
    is a leading indicator for voluntary departure.

    Result shape
    ------------
    One row per (department, termination_type), with the count of
    terminations, count flagged, and the share. Voluntary
    terminations are the main analytical interest; Layoff and
    Involuntary rows are included for completeness and contrast
    (layoffs are not driven by manager dynamics, so the rate should
    be at the random baseline).
*/

select
    department,
    termination_type,

    count(*)                                                        as terminations,
    countif(terminated_within_6mo_of_manager_change)                as flagged,
    round(
        safe_divide(
            countif(terminated_within_6mo_of_manager_change) * 100.0,
            count(*)
        ),
        1
    ) as flagged_pct_of_terminations,

    -- Tenure context: are these short-tenure or long-tenure departures?
    round(avg(tenure_at_termination_years), 2) as avg_tenure_years,

    -- Performance context: were they declining performers?
    countif(was_declining_performer) as declining_performers,
    round(
        safe_divide(
            countif(was_declining_performer) * 100.0,
            count(*)
        ),
        1
    ) as declining_pct
from {{ ref('fct_attrition_drivers') }}
group by department, termination_type
order by termination_type, flagged_pct_of_terminations desc
