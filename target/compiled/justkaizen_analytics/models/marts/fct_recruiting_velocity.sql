/*
    Model:        fct_recruiting_velocity
    Layer:        Mart — drill-through
    Materialized: table
    Grain:        One row per requisition_id
    Source:       int_recruiting_funnel_metrics (direct promotion)

    Purpose:
        Per-requisition drill-through for the hiring dashboard. When the
        CHRO sees elevated time-to-fill in fct_recruiting_reporting and
        asks "which reqs are taking the longest?", this is the table
        they filter.

    Notes:
        - Direct promotion from int_recruiting_funnel_metrics. The
          intermediate already exposes funnel volumes, conversion rates,
          time_to_fill, offer_acceptance_rate, and top_source per req.
*/

with funnel as (

    select * from `just-kaizen-ai`.`raw_intermediate`.`int_recruiting_funnel_metrics`

),

final as (

    select * from funnel

)

select * from final