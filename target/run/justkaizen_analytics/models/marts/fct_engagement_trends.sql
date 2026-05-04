
  
    

    create or replace table `just-kaizen-ai`.`raw_marts`.`fct_engagement_trends`
      
    
    

    
    OPTIONS()
    as (
      /*
    Model:        fct_engagement_trends
    Layer:        Mart
    Materialized: table
    Grain:        One row per (survey_cycle × department × theme)
    Source:       int_engagement_theme_rollup (direct promotion)

    Purpose:
        Theme-level engagement trends for Tableau. Anonymized data —
        no employee_id, so this mart cannot drill through to the
        individual roster. Department-level cuts only.

    Notes:
        - Direct promotion of the rollup intermediate. Already carries
          theme_avg_score, response_count, favorable_pct, enps_score
          (per cycle-dept), plus cycle-over-cycle deltas.
*/

with rollup_source as (

    select * from `just-kaizen-ai`.`raw_intermediate`.`int_engagement_theme_rollup`

),

final as (

    select * from rollup_source

)

select * from final
    );
  