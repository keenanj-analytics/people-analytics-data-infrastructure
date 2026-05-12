/*
    Model:        fct_recruiting_reporting
    Layer:        Mart
    Materialized: table
    Grain:        report_month × department × sub_department × job_level
                  × candidate_source × candidate_origin
                  × candidate_recruiter × candidate_hiring_manager
    Source:       int_reporting_grid_recruiting (scaffold)
                  stg_recruiting (activity), enriched with org dims from
                  int_employee_monthly_roster (hired) and stg_comp_bands
                  (non-hired).

    Purpose:
        Hiring metrics with TTM (12-month) and 3-month rolling windows,
        plus org-wide benchmarks at both horizons. Counts hires, offers
        extended / accepted / declined, and time-to-fill, sliced by
        department, sub_department, level, and the four recruiting-specific
        dimensions.

    Construction:
        1. Enrich each candidate with org dims:
             - Hired:     dept / sub_dept / level pulled from the latest
                          roster snapshot via employee_id.
             - Non-hired: dept / level pulled from stg_comp_bands via
                          job_title; sub_department from the hired candidate
                          on the same requisition (req_sub_department).
        2. Bucket each candidate to a report_month:
                COALESCE(DATE_TRUNC(start_date, MONTH),
                         DATE_TRUNC(offer_extended_date, MONTH))
           Hires bucket on start_date; declined / unaccepted offers fall
           back to the offer-extended date.
        3. Aggregate by the 7 grid dimensions × report_month.
        4. LEFT JOIN onto the recruiting grid via IS NOT DISTINCT FROM,
           COALESCE NULLs to 0.
        5. TTM (12-month) and 3-month rolling windows partitioned by all
           7 dimensions, ordered by report_month.
        6. Org-wide benchmarks (offer acceptance rate, avg time to fill)
           at both TTM and 3-month horizons, computed once per month from
           the full enriched candidate set and joined onto every cell.

    Excluded from time-to-fill:
        Internal hires (candidate_origin = 'internal') do not contribute
        to sum_time_to_fill, avg_time_to_fill, or the TTM time-to-fill
        denominator. They DO count toward total_hires.

    Non-hired sub_department:
        Non-hired candidates get sub_department from the hired candidate
        on the same requisition (req_sub_department CTE). This works for
        most reqs since they eventually fill. For unfilled reqs where no
        hire exists, sub_department remains NULL; the recruiting grid
        includes these NULL-sub_department combos so they still match
        the scaffold and are counted in cell-level acceptance rates.
*/

with recruiting as (

    select * from {{ ref('stg_recruiting') }}

),

employee_org_dims as (

    -- Latest org context per hired employee
    select distinct
        employee_id,
        department      as emp_department,
        sub_department  as emp_sub_department,
        job_level       as emp_job_level
    from {{ ref('int_employee_monthly_roster') }}

),

job_title_org_dims as (

    -- Org context for non-hired candidates via the requisition's job title
    select
        job_title,
        department      as title_department,
        job_level       as title_job_level
    from {{ ref('stg_comp_bands') }}

),

req_sub_department as (

    -- Sub-department for each requisition, derived from the hired candidate.
    -- Most requisitions that extended offers also made a hire, so the hired
    -- person's sub_department tells us which team the req was for. Non-hired
    -- candidates on the same req inherit this value.
    select distinct
        r.requisition_id,
        e.emp_sub_department        as req_sub_department
    from {{ ref('stg_recruiting') }} as r
    inner join employee_org_dims as e on r.employee_id = e.employee_id
    where r.employee_id is not null

),

candidates_enriched as (

    select
        r.candidate_id,
        r.requisition_id,
        r.outcome,
        r.application_channel       as candidate_source,
        r.origin                    as candidate_origin,
        r.recruiter                 as candidate_recruiter,
        r.hiring_manager            as candidate_hiring_manager,
        r.application_date,
        r.offer_extended_date,
        r.offer_accept_date,
        r.start_date,
        r.requisition_fill_start_date,
        r.time_to_fill_days,

        -- Org dims: roster (hired) takes precedence; comp_bands fallback
        coalesce(emp.emp_department, jt.title_department)   as department,
        coalesce(emp.emp_sub_department, req.req_sub_department) as sub_department,
        coalesce(emp.emp_job_level, jt.title_job_level)     as job_level,

        -- Bucket month
        coalesce(
            date_trunc(r.start_date, month),
            date_trunc(r.offer_extended_date, month)
        )                                                   as report_month,

        -- Event flags
        (r.outcome = 'Hired' and r.offer_accepted)          as is_hire,
        (r.offer_extended_date is not null)                 as offer_extended,
        (r.offer_accept_date is not null)                   as offer_accepted_flag,
        (r.offer_extended_date is not null
            and r.offer_accept_date is null)                as offer_declined_flag

    from recruiting as r
    left join employee_org_dims as emp  on r.employee_id    = emp.employee_id
    left join job_title_org_dims as jt  on r.job_title      = jt.job_title
    left join req_sub_department as req on r.requisition_id  = req.requisition_id

),

cell_aggregated as (

    select
        report_month,
        department,
        sub_department,
        job_level,
        candidate_source,
        candidate_origin,
        candidate_recruiter,
        candidate_hiring_manager,
        countif(is_hire)                                                                as total_hires,
        countif(offer_extended)                                                         as total_offers_extended,
        countif(offer_accepted_flag)                                                    as total_offers_accepted,
        countif(offer_declined_flag)                                                    as total_offers_declined,
        sum(case when is_hire and candidate_origin <> 'internal' then time_to_fill_days end)
                                                                                        as sum_time_to_fill,
        countif(is_hire and candidate_origin <> 'internal')                             as external_hires_for_ttf
    from candidates_enriched
    where report_month is not null
    group by 1, 2, 3, 4, 5, 6, 7, 8

),

scaffolded as (

    select
        g.report_month,
        g.report_quarter,
        g.department,
        g.sub_department,
        g.job_level,
        g.candidate_source,
        g.candidate_origin,
        g.candidate_recruiter,
        g.candidate_hiring_manager,
        coalesce(a.total_hires, 0)              as total_hires,
        coalesce(a.total_offers_extended, 0)    as total_offers_extended,
        coalesce(a.total_offers_accepted, 0)    as total_offers_accepted,
        coalesce(a.total_offers_declined, 0)    as total_offers_declined,
        coalesce(a.sum_time_to_fill, 0)         as sum_time_to_fill,
        coalesce(a.external_hires_for_ttf, 0)   as external_hires_for_ttf
    from {{ ref('int_reporting_grid_recruiting') }} as g
    left join cell_aggregated as a
        on  g.report_month                =                a.report_month
        and g.department              is not distinct from a.department
        and g.sub_department          is not distinct from a.sub_department
        and g.job_level               is not distinct from a.job_level
        and g.candidate_source        is not distinct from a.candidate_source
        and g.candidate_origin        is not distinct from a.candidate_origin
        and g.candidate_recruiter     is not distinct from a.candidate_recruiter
        and g.candidate_hiring_manager is not distinct from a.candidate_hiring_manager

),

with_ttm as (

    select
        *,
        -- 12-month (TTM) rolling
        sum(total_hires)            over cell_window_12m as ttm_total_hires,
        sum(total_offers_extended)  over cell_window_12m as ttm_offers_extended,
        sum(total_offers_accepted)  over cell_window_12m as ttm_offers_accepted,
        sum(sum_time_to_fill)       over cell_window_12m as ttm_sum_time_to_fill,
        sum(external_hires_for_ttf) over cell_window_12m as ttm_external_hires_for_ttf,

        -- 3-month rolling (for annualized quarterly rate)
        sum(total_hires)            over cell_window_3m  as r3m_total_hires,
        sum(total_offers_extended)  over cell_window_3m  as r3m_offers_extended,
        sum(total_offers_accepted)  over cell_window_3m  as r3m_offers_accepted,
        sum(sum_time_to_fill)       over cell_window_3m  as r3m_sum_time_to_fill,
        sum(external_hires_for_ttf) over cell_window_3m  as r3m_external_hires_for_ttf
    from scaffolded
    window
        cell_window_12m as (
            partition by
                department,
                sub_department,
                job_level,
                candidate_source,
                candidate_origin,
                candidate_recruiter,
                candidate_hiring_manager
            order by report_month
            rows between 11 preceding and current row
        ),
        cell_window_3m as (
            partition by
                department,
                sub_department,
                job_level,
                candidate_source,
                candidate_origin,
                candidate_recruiter,
                candidate_hiring_manager
            order by report_month
            rows between 2 preceding and current row
        )

),

orgwide_monthly as (

    select
        report_month,
        countif(offer_extended)                                                         as total_offers_extended,
        countif(offer_accepted_flag)                                                    as total_offers_accepted,
        sum(case when is_hire and candidate_origin <> 'internal' then time_to_fill_days end)
                                                                                        as sum_time_to_fill,
        countif(is_hire and candidate_origin <> 'internal')                             as external_hires_for_ttf
    from candidates_enriched
    where report_month is not null
    group by report_month

),

orgwide_ttm as (

    select
        report_month,
        -- 12-month (TTM)
        safe_divide(
            sum(total_offers_accepted)  over orgwide_12m,
            sum(total_offers_extended)  over orgwide_12m
        ) as orgwide_ttm_offer_acceptance_rate,
        safe_divide(
            sum(sum_time_to_fill)       over orgwide_12m,
            sum(external_hires_for_ttf) over orgwide_12m
        ) as orgwide_ttm_avg_time_to_fill,
        -- 3-month rolling
        safe_divide(
            sum(total_offers_accepted)  over orgwide_3m,
            sum(total_offers_extended)  over orgwide_3m
        ) as orgwide_r3m_offer_acceptance_rate,
        safe_divide(
            sum(sum_time_to_fill)       over orgwide_3m,
            sum(external_hires_for_ttf) over orgwide_3m
        ) as orgwide_r3m_avg_time_to_fill
    from orgwide_monthly
    window
        orgwide_12m as (
            order by report_month
            rows between 11 preceding and current row
        ),
        orgwide_3m as (
            order by report_month
            rows between 2 preceding and current row
        )

),

final as (

    select
        -- Time
        t.report_month,
        t.report_quarter,
        case
            when extract(month from t.report_month) in (3, 6, 9, 12)        then true
            when t.report_month = max(t.report_month) over ()               then true
            else false
        end                                                                 as flag_end_of_quarter,
        t.report_month = max(t.report_month) over ()                        as flag_latest_report,

        -- Dimensions
        t.department,
        t.sub_department,
        t.job_level,
        t.candidate_source,
        t.candidate_origin,
        t.candidate_recruiter,
        t.candidate_hiring_manager,

        -- Cell-level monthly metrics
        t.total_hires,
        t.total_offers_extended,
        t.total_offers_accepted,
        t.total_offers_declined,
        t.sum_time_to_fill,
        safe_divide(t.sum_time_to_fill, t.external_hires_for_ttf)           as avg_time_to_fill,

        -- Cell-level TTM (12-month rolling)
        t.ttm_total_hires,
        t.ttm_offers_extended,
        t.ttm_offers_accepted,
        t.ttm_sum_time_to_fill,
        t.ttm_external_hires_for_ttf,
        safe_divide(t.ttm_offers_accepted,    t.ttm_offers_extended)        as ttm_offer_acceptance_rate,
        safe_divide(t.ttm_sum_time_to_fill,   t.ttm_external_hires_for_ttf) as ttm_avg_time_to_fill,

        -- Cell-level 3-month rolling
        t.r3m_total_hires,
        t.r3m_offers_extended,
        t.r3m_offers_accepted,
        t.r3m_sum_time_to_fill,
        t.r3m_external_hires_for_ttf,
        safe_divide(t.r3m_offers_accepted,    t.r3m_offers_extended)        as r3m_offer_acceptance_rate,
        safe_divide(t.r3m_sum_time_to_fill,   t.r3m_external_hires_for_ttf) as r3m_avg_time_to_fill,

        -- Org-wide TTM benchmarks
        o.orgwide_ttm_offer_acceptance_rate,
        o.orgwide_ttm_avg_time_to_fill,

        -- Org-wide 3-month rolling benchmarks
        o.orgwide_r3m_offer_acceptance_rate,
        o.orgwide_r3m_avg_time_to_fill

    from with_ttm as t
    left join orgwide_ttm as o
        on t.report_month = o.report_month

)

select * from final
