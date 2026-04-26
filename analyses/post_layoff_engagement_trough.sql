/*
    Analysis:  Post-layoff engagement trough by theme
    Mart:      fct_engagement_trends
    Audience:  CEO + Chief People Officer, post-mortem on the Q1 2023 layoff

    Question
    --------
    "Across the 8 engagement themes, how much did each one drop
    between the pre-layoff cycle (2022-Q4) and the trauma trough
    (2023-Q2)? Has the recovery to 2025-Q1 fully closed the gap?
    Surface the themes where recovery is incomplete."

    Why these three cycles
    ----------------------
    Section 10's narrative places:
        2022-Q4 = pre-layoff drift (slight decline from 2022-Q2 peak)
        2023-Q1 = layoff hits
        2023-Q2 = post-layoff trauma trough (deepest dip)
        2024 onward = gradual recovery
        2025-Q1 = stabilized near pre-layoff levels

    Comparing 2022-Q4 -> 2023-Q2 -> 2025-Q1 shows the size of the
    drop and how much of it has been clawed back.

    Result shape
    ------------
    One row per (department, theme), with three score columns
    (pre-layoff, trough, current) plus two delta columns (drop and
    recovery_gap). Order by recovery_gap descending: themes still
    underperforming pre-layoff levels rise to the top.
*/

with pre_layoff as (
    select department, theme, theme_avg_score
    from {{ ref('fct_engagement_trends') }}
    where survey_cycle = '2022-Q4'
),

trough as (
    select department, theme, theme_avg_score
    from {{ ref('fct_engagement_trends') }}
    where survey_cycle = '2023-Q2'
),

current_state as (
    select department, theme, theme_avg_score
    from {{ ref('fct_engagement_trends') }}
    where survey_cycle = '2025-Q1'
)

select
    p.department,
    p.theme,
    p.theme_avg_score as pre_layoff_2022q4,
    t.theme_avg_score as trough_2023q2,
    c.theme_avg_score as current_2025q1,

    -- Drop from pre-layoff to trough (negative = drop)
    round(t.theme_avg_score - p.theme_avg_score, 2) as drop_during_layoff,

    -- Recovery gap: how far below pre-layoff is current state?
    -- Negative or zero = fully recovered. Positive = still below.
    round(p.theme_avg_score - c.theme_avg_score, 2) as recovery_gap,

    -- Recovery completeness: 1.00 = fully recovered, < 1 = partial
    round(
        safe_divide(
            c.theme_avg_score - t.theme_avg_score,
            p.theme_avg_score - t.theme_avg_score
        ),
        2
    ) as recovery_share
from pre_layoff   as p
join trough       as t on p.department = t.department and p.theme = t.theme
join current_state as c on p.department = c.department and p.theme = c.theme
order by recovery_gap desc, drop_during_layoff
