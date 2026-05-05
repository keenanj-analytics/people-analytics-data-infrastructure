# JustKaizen AI: dbt Model Architecture Spec v2

## Overview

This spec defines the complete dbt model architecture for JustKaizen AI's People Analytics data warehouse. It replaces the v1 architecture (which used a single fct_monthly_metrics table with segment_type/segment_value pivoting) with a production-grade design based on domain-specific reporting marts with all dimensions as columns.

This design is modeled after a production People Analytics infrastructure built at a ~950-person tech company, adapted for dbt and BigQuery.

## Design Principles

1. **The employee monthly roster is the center of everything.** One row per employee per active month. Every reporting mart aggregates from it. Every drill-through reads from it.
2. **Domain-specific reporting marts.** Each business domain (attrition, recruiting, workforce composition, compensation) has its own mart with only the dimensions relevant to that domain.
3. **Full time grid with no gaps.** A daily calendar table (dim_calendar) and per-domain reporting grids ensure every month has a row, even with zero activity. Trend lines never have gaps.
4. **Org-wide and department benchmarks on every row.** Each mart carries segment-level, department-level, and org-wide metrics so any row can be compared to its parent benchmark without a second data source.
5. **Row-based, not column-based.** Metrics are columns. Dimensions are rows. Adding a new department doesn't require rebuilding the model.
6. **Pre-computed rolling windows.** TTM (trailing 12 months) attrition rates, time-to-fill averages, and offer acceptance rates are computed in SQL using `ROWS BETWEEN 11 PRECEDING AND CURRENT ROW`. Tableau does not compute any rates.

---

## Raw Source Tables (Seeds)

No changes to existing seeds. Current schema supports the new architecture.

| Seed | Key Columns for New Architecture |
|------|----------------------------------|
| raw_employees | employee_id, department, sub_department, job_level, job_title, hire_date, termination_date, termination_type, termination_reason, employment_status, manager_id, gender, race_ethnicity, is_critical_talent |
| raw_job_history | employee_id, effective_date, change_type, old/new department, sub_department, job_level, job_title, manager_id |
| raw_compensation | employee_id, salary, comp_band_min, comp_band_mid, comp_band_max, effective_date |
| raw_performance | employee_id, review_cycle, overall_rating, manager_rating, review_completed_date |
| raw_recruiting | requisition_id, application_id, department, sub_department, recruiter, hiring_manager, source, application_date, offer_date, hire_date, offer_accepted |
| raw_engagement | survey_cycle, department, sub_department, theme, avg_score, enps_score (anonymized, no employee_id) |

---

## Model Inventory

### Models to KEEP (no changes)

| Layer | Model | Reason |
|-------|-------|--------|
| Staging | stg_employees | Clean rename, no business logic. Still needed. |
| Staging | stg_job_history | Clean rename. Still needed. |
| Staging | stg_compensation | Clean rename. Still needed. |
| Staging | stg_performance | Clean rename. Still needed. |
| Staging | stg_recruiting | Clean rename. Still needed. |
| Staging | stg_engagement | Clean rename. Still needed. |
| Intermediate | int_employee_tenure | tenure_months, promotions, career_velocity. Feeds into roster. |
| Intermediate | int_employee_compensation_current | Latest comp per employee. Feeds into roster. |
| Intermediate | int_employee_performance_history | Rating history, is_top_performer. Feeds into roster. |
| Intermediate | int_recruiting_funnel_metrics | Per-requisition funnel. Feeds into fct_recruiting_velocity. |
| Intermediate | int_engagement_theme_rollup | Theme rollup by department. Feeds into fct_engagement_trends. |
| Mart | fct_recruiting_velocity | Per-requisition detail table. Stays for drill-through. |
| Mart | fct_engagement_trends | Theme-level by dept and cycle. Stays (engagement is anonymized, can't join to roster). |
| Mart | fct_performance_distribution | Per employee x cycle ratings. Stays for drill-through. |

### Models to REMOVE

| Layer | Model | Reason |
|-------|-------|--------|
| Mart | fct_monthly_metrics | Replaced by 4 domain-specific reporting marts |
| Intermediate | int_monthly_headcount_snapshot | Replaced by int_employee_monthly_roster |
| Intermediate | int_employee_dimension | Functionality absorbed into int_employee_monthly_roster |
| Intermediate | int_employee_event_sequence | Not needed for new architecture |
| Mart | fct_workforce_overview | Replaced by fct_workforce_composition |
| Mart | fct_attrition_drivers | Replaced by fct_employee_roster (drill-through) + fct_attrition_reporting (aggregation) |
| Mart | fct_compensation_parity | Replaced by fct_compensation_reporting (aggregated) + fct_employee_roster (individual detail) |

### Models to ADD

| Layer | Model | Purpose |
|-------|-------|---------|
| Seed/Model | dim_calendar | Daily date spine (Jan 1, 2020 through Dec 31, 2026). Centralized date reference. |
| Intermediate | int_employee_monthly_roster | One row per employee per month. The golden record. All dimensions as columns. |
| Intermediate | int_reporting_grid_attrition | Full month x dimension scaffold for attrition mart |
| Intermediate | int_reporting_grid_recruiting | Full month x dimension scaffold for recruiting mart |
| Intermediate | int_reporting_grid_workforce | Full month x dimension scaffold for workforce mart |
| Intermediate | int_reporting_grid_compensation | Full month x dimension scaffold for compensation mart |
| Mart | fct_attrition_reporting | Rolling 12mo attrition rates across all dimension combinations |
| Mart | fct_recruiting_reporting | Rolling 12mo hiring speed and offer acceptance metrics |
| Mart | fct_workforce_composition | Headcount, representation, span of control by all dimensions |
| Mart | fct_compensation_reporting | Compa-ratio, salary distribution by all dimensions |
| Mart | fct_employee_roster | The monthly roster promoted to mart for Tableau drill-through |

**Updated model count: ~29 models** (6 staging + 12 intermediate + 11 marts)

---

## Model Specifications

### dim_calendar

**Type:** dbt model (generated, not seed)
**Grain:** One row per day
**Range:** January 1, 2020 through December 31, 2026

| Column | Type | Logic |
|--------|------|-------|
| calendar_date | DATE | Every day in range |
| report_month | DATE | DATE_TRUNC(calendar_date, MONTH) |
| report_quarter | STRING | FORMAT: "2025 Q1" |
| report_year | INT | EXTRACT(YEAR) |
| day_of_month | INT | EXTRACT(DAY) |
| is_month_start | BOOL | day_of_month = 1 |
| is_month_end | BOOL | calendar_date = LAST_DAY(calendar_date) |
| is_quarter_end | BOOL | is_month_end AND EXTRACT(MONTH) IN (3, 6, 9, 12) |
| flag_latest_month | BOOL | report_month = MAX(report_month) across all months with employee data |

---

### int_employee_monthly_roster

**Type:** Intermediate view
**Grain:** One row per employee per month they were active (or terminated in that month)
**Source:** stg_employees, dim_calendar, stg_job_history, int_employee_compensation_current, int_employee_performance_history, stg_recruiting

**Construction logic:**

1. Cross join stg_employees with dim_calendar (distinct report_months only)
2. Filter: employee appears in a month if `hire_date <= LAST_DAY(report_month) AND (termination_date IS NULL OR termination_date >= DATE_TRUNC(report_month, MONTH))`
3. For each employee x month row, attach:
   - Current-as-of-that-month attributes from stg_job_history (latest job_history event on or before the last day of the month)
   - Compensation from int_employee_compensation_current (latest comp record on or before report_month)
   - Performance from int_employee_performance_history (latest completed review on or before report_month)
   - Pre-hire fields from stg_recruiting (joined via employee_id link or requisition match)

| Column | Type | Source / Logic |
|--------|------|----------------|
| employee_id | STRING | stg_employees |
| report_month | DATE | dim_calendar |
| report_quarter | STRING | dim_calendar |
| first_name | STRING | stg_employees |
| last_name | STRING | stg_employees |
| email | STRING | stg_employees |
| department | STRING | Point-in-time from job_history, falling back to stg_employees |
| sub_department | STRING | Point-in-time from job_history |
| job_title | STRING | Point-in-time from job_history |
| job_level | STRING | Point-in-time from job_history |
| level_group | STRING | CASE: P1-P3 = "Junior IC", P4-P6 = "Senior IC", M1-M4 = "Manager", M5-M6 = "Director", E1-E6 = "Executive" |
| manager_id | STRING | Point-in-time from job_history |
| hire_date | DATE | stg_employees |
| termination_date | DATE | stg_employees (NULL for active) |
| termination_type | STRING | stg_employees (NULL for active) |
| termination_reason | STRING | stg_employees (NULL for active) |
| employment_status | STRING | "Active" if report_month < termination_month, "Terminated" if report_month = termination_month |
| tenure_months | INT | DATE_DIFF(LAST_DAY(report_month), hire_date, MONTH) |
| tenure_bucket | STRING | CASE: 0-1 Years, 1-2 Years, 2-3 Years, 3-4 Years, 4-5 Years, 5+ Years. Boundary: exactly 12 months = "1-2 Years" (exclusive lower, inclusive upper) |
| new_hire_flag | STRING | "New Hire" if tenure_months <= 12, else "Tenured" |
| gender | STRING | stg_employees |
| race_ethnicity | STRING | stg_employees |
| location_state | STRING | stg_employees |
| is_critical_talent | BOOL | stg_employees |
| salary | FLOAT | int_employee_compensation_current (latest as of report_month) |
| comp_band_min | FLOAT | int_employee_compensation_current |
| comp_band_mid | FLOAT | int_employee_compensation_current |
| comp_band_max | FLOAT | int_employee_compensation_current |
| compa_ratio | FLOAT | ROUND(salary / comp_band_mid, 2) |
| latest_performance_rating | STRING | int_employee_performance_history (most recent completed review as of report_month) |
| latest_performance_rating_numeric | INT | Mapped: "Significantly Exceeds" = 5, "Exceeds" = 4, "Meets" = 3, "Partially Meets" = 2, "Does Not Meet" = 1 |
| latest_performance_rating_description | STRING | Full text of the rating |
| top_performer_flag | STRING | "Y" if latest_performance_rating_numeric >= 4 OR is_critical_talent = TRUE, else "N" |
| candidate_source | STRING | stg_recruiting (joined via employee_id or hire linkage) |
| candidate_origin | STRING | stg_recruiting (applied/sourced/referred/agency/internal) |
| candidate_recruiter | STRING | stg_recruiting |
| candidate_hiring_manager | STRING | stg_recruiting |
| no_direct_reports | INT | COUNT of employees where manager_id = this employee_id in the same report_month |
| manager_status | BOOL | TRUE if no_direct_reports > 0 OR job_level starts with "M" or "E" |
| flag_latest_report | STRING | "X" if report_month = MAX(report_month) across the table |
| flag_month_beg_end | STRING | Always "End" (we use end-of-month snapshots only; beginning of month derived from prior month's end) |
| is_terminated_this_month | BOOL | TRUE if DATE_TRUNC(termination_date, MONTH) = report_month |
| is_excluded_termination | BOOL | TRUE if termination_reason IN ('Reduction in Force', ...) (standard exclusion list) |
| is_attrition_eligible_term | BOOL | is_terminated_this_month = TRUE AND is_excluded_termination = FALSE |

**Notes:**
- Terminated employees appear in the roster for the month they were terminated, then drop off. This ensures the termination count is captured in the correct month.
- Attributes reflect the state at the END of the month (or at termination, whichever comes first).
- The roster is the ONLY place where dimensions are derived. Reporting marts read dimensions from the roster, never re-derive them.

---

### Reporting Grids (4 models)

Each grid ensures no gaps in trend lines. Construction pattern:

1. Get all distinct report_months from dim_calendar (filtered to months that have at least one employee in the roster)
2. Get all distinct dimension combinations that have ever existed in the roster (not every theoretically possible combination, but every combination that has had at least one employee)
3. CROSS JOIN months x dimension combinations
4. The reporting mart LEFT JOINs actual activity onto this grid

**int_reporting_grid_attrition**

Grain: report_month x department x sub_department x job_level x level_group x tenure_bucket x top_performer_flag x gender x race_ethnicity

**int_reporting_grid_recruiting**

Grain: report_month x department x sub_department x job_level x candidate_source x candidate_origin x candidate_recruiter x candidate_hiring_manager

**int_reporting_grid_workforce**

Grain: report_month x department x sub_department x job_level x level_group x gender x race_ethnicity x manager_status

**int_reporting_grid_compensation**

Grain: report_month x department x sub_department x job_level x level_group x gender x latest_performance_rating

---

### fct_attrition_reporting

**Type:** Mart (table)
**Grain:** One row per (report_month x dimension combination from attrition grid)
**Source:** int_reporting_grid_attrition LEFT JOIN aggregated roster data

| Column | Type | Logic |
|--------|------|-------|
| report_month | DATE | From grid |
| report_quarter | STRING | "2025 Q1" format |
| flag_end_of_quarter | BOOL | Month is 3, 6, 9, 12 or is latest month |
| flag_latest_report | BOOL | report_month = MAX(report_month) |
| department | STRING | Dimension |
| sub_department | STRING | Dimension |
| job_level | STRING | Dimension |
| level_group | STRING | Dimension |
| tenure_bucket | STRING | Dimension |
| top_performer_flag | STRING | Dimension |
| gender | STRING | Dimension |
| race_ethnicity | STRING | Dimension |
| beg_month_headcount | INT | Count of active employees at start of month (prior month end count) |
| end_month_headcount | INT | Count of active employees at end of month |
| avg_headcount | FLOAT | (beg + end) / 2 |
| total_terminations | INT | Count where is_attrition_eligible_term = TRUE |
| voluntary_terminations | INT | Count where termination_type = "Voluntary" AND is_attrition_eligible_term = TRUE |
| involuntary_terminations | INT | Count where termination_type = "Involuntary" AND is_attrition_eligible_term = TRUE |
| top_performer_terminations | INT | Count where top_performer_flag = "Y" AND is_attrition_eligible_term = TRUE |
| ttm_total_terminations | INT | SUM(total_terminations) OVER (ORDER BY report_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) |
| ttm_voluntary_terminations | INT | Same window for voluntary |
| ttm_avg_headcount | FLOAT | AVG(end_month_headcount) OVER (ORDER BY report_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) |
| ttm_overall_attrition_rate | FLOAT | ttm_total_terminations / ttm_avg_headcount |
| ttm_voluntary_attrition_rate | FLOAT | ttm_voluntary_terminations / ttm_avg_headcount |
| ttm_top_performer_attrition_rate | FLOAT | ttm_top_performer_terminations / ttm_avg_headcount |
| orgwide_ttm_overall_attrition_rate | FLOAT | Company-wide TTM overall attrition for the same month |
| orgwide_ttm_voluntary_attrition_rate | FLOAT | Company-wide TTM voluntary attrition for the same month |
| dept_ttm_overall_attrition_rate | FLOAT | Department-level TTM overall attrition for the same month |

**Excluded termination reasons (filtered via is_attrition_eligible_term):**
Reduction in Force, End of Contract, Entity Change, Acquisition/Merger, End of Internship, International Transfer, Relocation, Converting to FT

---

### fct_recruiting_reporting

**Type:** Mart (table)
**Grain:** One row per (report_month x dimension combination from recruiting grid)
**Source:** int_reporting_grid_recruiting LEFT JOIN aggregated recruiting data

| Column | Type | Logic |
|--------|------|-------|
| report_month | DATE | From grid (based on hire_date or offer_date) |
| report_quarter | STRING | |
| flag_end_of_quarter | BOOL | |
| flag_latest_report | BOOL | |
| department | STRING | Dimension |
| sub_department | STRING | Dimension |
| job_level | STRING | Dimension (from the requisition, not the employee) |
| candidate_source | STRING | Dimension |
| candidate_origin | STRING | Dimension |
| candidate_recruiter | STRING | Dimension |
| candidate_hiring_manager | STRING | Dimension |
| total_hires | INT | Count of hires in this month x dimension |
| total_offers_extended | INT | Count of offers (accepted + declined) |
| total_offers_accepted | INT | Count where offer_accepted = TRUE |
| total_offers_declined | INT | Count where offer_accepted = FALSE |
| sum_time_to_fill | FLOAT | SUM(DATE_DIFF(hire_date, application_date, DAY)) for hires |
| avg_time_to_fill | FLOAT | sum_time_to_fill / total_hires |
| ttm_total_hires | INT | SUM(total_hires) OVER (ROWS BETWEEN 11 PRECEDING AND CURRENT ROW) |
| ttm_offers_extended | INT | Same window |
| ttm_offers_accepted | INT | Same window |
| ttm_offer_acceptance_rate | FLOAT | ttm_offers_accepted / ttm_offers_extended |
| ttm_avg_time_to_fill | FLOAT | SUM(sum_time_to_fill) OVER (11 preceding) / ttm_total_hires |
| orgwide_ttm_offer_acceptance_rate | FLOAT | Company-wide TTM offer acceptance for same month |
| orgwide_ttm_avg_time_to_fill | FLOAT | Company-wide TTM avg time to fill for same month |

**Note:** Internal hires (candidate_origin = "internal") excluded from time-to-fill calculations.

---

### fct_workforce_composition

**Type:** Mart (table)
**Grain:** One row per (report_month x dimension combination from workforce grid)
**Source:** int_reporting_grid_workforce LEFT JOIN aggregated roster data

| Column | Type | Logic |
|--------|------|-------|
| report_month | DATE | From grid |
| report_quarter | STRING | |
| flag_end_of_quarter | BOOL | |
| flag_latest_report | BOOL | |
| department | STRING | Dimension |
| sub_department | STRING | Dimension |
| job_level | STRING | Dimension |
| level_group | STRING | Dimension |
| gender | STRING | Dimension |
| race_ethnicity | STRING | Dimension |
| manager_status | BOOL | Dimension |
| headcount | INT | COUNT DISTINCT employee_id where employment_status = "Active" |
| hires | INT | COUNT where hire_date falls in this month |
| terminations | INT | COUNT where is_terminated_this_month = TRUE |
| net_change | INT | hires - terminations |
| pct_of_total_headcount | FLOAT | headcount / orgwide_headcount for same month |
| gender_representation_rate | FLOAT | headcount / department_headcount for same month (used for gender within dept) |
| race_representation_rate | FLOAT | Same pattern for race |
| avg_span_of_control | FLOAT | AVG(no_direct_reports) where manager_status = TRUE |
| avg_tenure_months | FLOAT | AVG(tenure_months) |
| orgwide_headcount | INT | Company-wide headcount for same month |
| orgwide_avg_span_of_control | FLOAT | Company-wide avg span for same month |

---

### fct_compensation_reporting

**Type:** Mart (table)
**Grain:** One row per (report_month x dimension combination from compensation grid)
**Source:** int_reporting_grid_compensation LEFT JOIN aggregated roster data

| Column | Type | Logic |
|--------|------|-------|
| report_month | DATE | From grid |
| report_quarter | STRING | |
| flag_end_of_quarter | BOOL | |
| flag_latest_report | BOOL | |
| department | STRING | Dimension |
| sub_department | STRING | Dimension |
| job_level | STRING | Dimension |
| level_group | STRING | Dimension |
| gender | STRING | Dimension |
| latest_performance_rating | STRING | Dimension |
| employee_count | INT | COUNT of active employees in this cell |
| avg_salary | FLOAT | AVG(salary) |
| avg_compa_ratio | FLOAT | AVG(compa_ratio) |
| avg_band_position | FLOAT | AVG((salary - comp_band_min) / (comp_band_max - comp_band_min)) |
| count_below_band | INT | COUNT where compa_ratio < 0.90 |
| count_above_band | INT | COUNT where compa_ratio > 1.10 |
| median_compa_ratio | FLOAT | APPROX_QUANTILES(compa_ratio, 2)[OFFSET(1)] |
| orgwide_avg_compa_ratio | FLOAT | Company-wide avg compa_ratio for same month |
| orgwide_avg_salary | FLOAT | Company-wide avg salary for same month |
| dept_avg_compa_ratio | FLOAT | Department-level avg compa_ratio for same month |

---

### fct_employee_roster

**Type:** Mart (table)
**Grain:** Same as int_employee_monthly_roster (one row per employee per month)
**Source:** int_employee_monthly_roster (direct promotion, possibly with additional calculated fields)

This is the drill-through table. When a CHRO sees elevated attrition in Engineering and asks "who left?", this is where they look. When someone asks "who's below band?", filter this table to compa_ratio < 0.90.

All columns from int_employee_monthly_roster, plus:

| Column | Type | Logic |
|--------|------|-------|
| full_name | STRING | CONCAT(first_name, ' ', last_name) |
| is_active | BOOL | employment_status = "Active" |
| band_position_label | STRING | CASE: "Below Band" if compa_ratio < 0.90, "Within Band" if 0.90-1.10, "Above Band" if > 1.10 |

---

## Dependency Graph

```
Seeds (raw_*)
  └── Staging (stg_*)
        └── dim_calendar
        └── Existing intermediates (int_employee_tenure, int_employee_compensation_current, int_employee_performance_history)
              └── int_employee_monthly_roster
                    ├── int_reporting_grid_attrition ──► fct_attrition_reporting
                    ├── int_reporting_grid_recruiting ──► fct_recruiting_reporting
                    ├── int_reporting_grid_workforce ──► fct_workforce_composition
                    ├── int_reporting_grid_compensation ──► fct_compensation_reporting
                    └── fct_employee_roster (direct promotion)
        └── int_recruiting_funnel_metrics ──► fct_recruiting_velocity
        └── int_engagement_theme_rollup ──► fct_engagement_trends
        └── int_employee_performance_history ──► fct_performance_distribution
```

---

## Dashboard Data Source Mapping (Updated)

| Tableau View | Primary Data Source | Drill-through Source |
|-------------|-------------------|---------------------|
| Workforce Overview | fct_workforce_composition | fct_employee_roster |
| Attrition | fct_attrition_reporting | fct_employee_roster (filter: is_terminated_this_month) |
| Hiring | fct_recruiting_reporting | fct_recruiting_velocity |
| Compensation | fct_compensation_reporting | fct_employee_roster (filter: is_active, latest month) |
| Engagement | fct_engagement_trends | N/A (anonymized) |
| Performance | fct_performance_distribution | fct_employee_roster |

---

## Testing Strategy

Each reporting mart should have:
- **Row count validation:** Grid should produce rows for every month x dimension combo that ever existed
- **Sum validation:** SUM(headcount) across all department rows for a given month should equal the org-wide headcount
- **Rate bounds:** attrition rates between 0 and 1 (or 0 and 100%), compa-ratios between 0.5 and 2.0
- **No NULLs in dimensions:** All dimension columns should have values (use "Unknown" or "N/A" for missing)
- **TTM window validation:** For the first 11 months, TTM calculations use fewer than 12 months. Flag these or start the time series 12 months after the earliest data.
