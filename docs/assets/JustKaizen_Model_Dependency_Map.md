# JustKaizen AI: Model Dependency Map

## How to Read This

Each section shows: **Target model** ← what feeds into it, with the join key and what it provides.

---

## Staging → Intermediate Connections

### int_employee_tenure

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| stg_employees | employee_id | hire_date, termination_date (used to compute tenure_months) |
| stg_job_history* | employee_id | Job change events (used to count promotions, career velocity) |

*Note: If raw_job_history is eliminated and job changes are tracked via monthly snapshots, this model derives tenure directly from stg_employees and may be absorbed into the roster.*

---

### int_employee_compensation_current

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| stg_employees | employee_id | Current salary (from the latest Report_Date snapshot) |
| stg_comp_bands | job_title = title + pay_zone determines Zone A vs Zone B | comp_band_min, comp_band_mid, comp_band_max |

**Output:** One row per employee with their latest salary and matched comp band. Computes compa_ratio = salary / comp_band_mid.

*Note: If raw_employees already has one salary per employee per Report_Date, this model may simplify to just the comp band join.*

---

### int_employee_performance_history

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| stg_performance | employee_id | All review cycle ratings (already filtered to manager + Performance Category in staging) |

**Output:** One row per employee with their latest_perf_rating, latest_perf_rating_numeric, rating trend (improving/declining/stable), and is_top_performer flag.

**Logic:** Picks the most recent completed review per employee. Computes top_performer_flag = "Y" if rating_numeric >= 4 OR is_critical_talent = TRUE.

---

### int_employee_monthly_roster (THE GOLDEN RECORD)

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| stg_employees | employee_id | All employee attributes: name, email, department, sub_department, team, job_title, job_level, hire_date, termination_date, termination_reason, termination_voluntary, termination_regrettable, gender, race, location, pay_zone, manager_email, critical_talent, employment_type |
| dim_calendar | CROSS JOIN on distinct report_months, filtered by hire_date <= month_end AND (termination_date IS NULL OR termination_date >= month_start) | Creates one row per employee per month they existed |
| int_employee_compensation_current | employee_id | salary, comp_band_min, comp_band_mid, comp_band_max, compa_ratio |
| int_employee_performance_history | employee_id | latest_perf_rating, latest_perf_rating_numeric, top_performer_flag |
| stg_recruiting | employee_id (for hired candidates only) | candidate_source, candidate_origin, candidate_recruiter, candidate_hiring_manager |

**Derived fields (computed in this model, not from any source):**
- level_group: CASE on job_level (P1-P3=Junior IC, P4-P6=Senior IC, M1-M2=Manager, M3-M4=Director, E1-E6=Senior Leadership)
- tenure_months: DATE_DIFF(report_month_end, hire_date, MONTH)
- tenure_bucket: CASE on tenure_months (0-1 Years through 5+ Years)
- new_hire_flag: "New Hire" if tenure <= 12 months, "Tenured" otherwise
- employment_status: "Active" or "Terminated" based on report_month vs termination_month
- termination_type: "Voluntary" if termination_voluntary = TRUE, "Involuntary" if FALSE
- is_terminated_this_month: TRUE if termination_month = report_month
- is_excluded_termination: TRUE if termination_reason IN (excluded list)
- is_attrition_eligible_term: is_terminated_this_month AND NOT is_excluded_termination
- no_direct_reports: COUNT of employees where manager_id = this employee in same month
- manager_status: from stg_employees
- flag_latest_report: "X" if report_month = MAX(report_month)

---

### int_recruiting_funnel_metrics

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| stg_recruiting | requisition_id | All candidate records: application dates, stage dates, outcomes, sources, recruiters, hiring managers |

**Output:** One row per requisition with funnel volumes (total applicants, screened, interviewed, offered, hired), conversion rates between stages, time_to_fill, offer_acceptance_rate, and top_source.

**No connection to the roster.** This is a separate path.

---

### int_engagement_theme_rollup

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| stg_engagement | No join key (aggregation only) | Individual survey responses with question text, theme, likert score, eNPS, department, demographic cuts |

**Output:** Aggregated to (survey_cycle x department x theme) grain. Computes theme_avg_score, response_count, favorable_pct, enps_score, and cycle-over-cycle deltas.

**No connection to the roster.** This is a separate path. Data is anonymized.

---

### int_reporting_grid_attrition

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| dim_calendar | Distinct report_months | Every month in the date spine |
| int_employee_monthly_roster | Distinct dimension combos (department, sub_department, job_level, level_group, tenure_bucket, top_performer_flag, gender, race_ethnicity) | Every combination that has ever had at least one employee |

**Output:** CROSS JOIN of months x dimension combos. One row per possible cell. This is the scaffold that prevents gaps in trend lines.

**Logic:** `SELECT DISTINCT report_month FROM dim_calendar CROSS JOIN (SELECT DISTINCT dept, sub_dept, level... FROM roster)`

---

### int_reporting_grid_recruiting

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| dim_calendar | Distinct report_months | Every month |
| int_employee_monthly_roster | Distinct dimension combos for hired candidates (always has sub_department) | Roster-based combos |
| raw_offers_hires + stg_comp_bands | Distinct dimension combos for all candidates (department/job_level from comp_bands, sub_department from requisition hire lookup) | Candidate-based combos (includes non-hired) |
| stg_recruiting | Requisition → employee_id lookup for sub_department | Links requisitions to hired employees |

**Output:** UNION of roster-based and candidate-based dimension combos CROSS JOINed with calendar months. Ensures declined offers (which have NULL sub_department from comp_bands but populated sub_department from req lookup) survive the scaffold join.

---

### int_reporting_grid_workforce

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| dim_calendar | Distinct report_months | Every month |
| int_employee_monthly_roster | Distinct dimension combos (department, sub_department, job_level, level_group, gender, race_ethnicity, manager_status) | Every combo that has existed |

**Output:** Scaffold for workforce composition mart.

---

### int_reporting_grid_compensation

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| dim_calendar | Distinct report_months | Every month |
| int_employee_monthly_roster | Distinct dimension combos (department, sub_department, job_level, level_group, gender, latest_performance_rating) | Every combo that has existed |

**Output:** Scaffold for compensation mart.

---

## Intermediate → Mart Connections

### fct_attrition_reporting

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| int_reporting_grid_attrition | report_month + all dimension columns | The scaffold (every month x dimension combo, including zeros) |
| int_employee_monthly_roster | report_month + matching dimensions (GROUP BY) | Aggregated: end_month_headcount, total_terminations, voluntary_terminations, involuntary_terminations, top_performer_terminations, regrettable_terminations |

**Logic:**
1. Aggregate the roster by grid dimensions (COUNT headcount, COUNT terminations by type)
2. LEFT JOIN onto the grid (grid provides the scaffold, aggregated roster provides the numbers)
3. Compute rolling windows: SUM(terminations) OVER 12 months, AVG(headcount) OVER 12 months
4. Compute rates: ttm_attrition_rate = ttm_terminations / ttm_avg_headcount
5. Join org-wide and department-level rates back onto every row for benchmarking

---

### fct_recruiting_reporting

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| int_reporting_grid_recruiting | report_month + all dimension columns | The scaffold |
| stg_recruiting | hire_month (derived from Candidate_Start_Date) + matching dimensions (GROUP BY) | Aggregated: total_hires, total_offers_extended, total_offers_accepted, total_offers_declined, sum_time_to_fill |

**Logic:**
1. Aggregate recruiting data by grid dimensions
2. LEFT JOIN onto the grid
3. Compute rolling windows: SUM(hires) OVER 12 months, SUM(offers) OVER 12 months
4. Compute rates: ttm_offer_acceptance_rate, ttm_avg_time_to_fill
5. Join org-wide rates for benchmarking

**Note:** Internal hires (origin = "internal") excluded from time-to-fill calculations.

---

### fct_workforce_composition

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| int_reporting_grid_workforce | report_month + all dimension columns | The scaffold |
| int_employee_monthly_roster | report_month + matching dimensions (GROUP BY) | Aggregated: headcount, hires, terminations, net_change, avg_tenure_months, avg_span_of_control |

**Logic:**
1. Aggregate the roster by grid dimensions
2. LEFT JOIN onto the grid
3. Compute representation rates: pct_of_total_headcount, gender_representation_rate, race_representation_rate
4. Join org-wide headcount and span of control for benchmarking

---

### fct_compensation_reporting

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| int_reporting_grid_compensation | report_month + all dimension columns | The scaffold |
| int_employee_monthly_roster | report_month + matching dimensions (GROUP BY) | Aggregated: employee_count, avg_salary, avg_compa_ratio, avg_band_position, count_below_band, count_above_band, median_compa_ratio |

**Logic:**
1. Aggregate the roster by grid dimensions (only active employees, only Full Time)
2. LEFT JOIN onto the grid
3. Join org-wide and department-level comp benchmarks for comparison

---

### fct_employee_roster (DRILL-THROUGH)

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| int_employee_monthly_roster | Direct pass-through (no join) | All columns from the roster |

**Logic:** SELECT * FROM int_employee_monthly_roster, plus:
- full_name = CONCAT(first_name, ' ', last_name)
- band_position_label = CASE on compa_ratio (Below/Within/Above Band)
- is_active = employment_status = "Active"

**Materialized as TABLE** (not view) so Tableau queries are fast.

---

### fct_recruiting_velocity (DRILL-THROUGH)

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| int_recruiting_funnel_metrics | Direct pass-through | Per-requisition funnel detail |

**Purpose:** When the CHRO asks "which reqs are taking the longest?", this is the drill-through table.

---

### fct_engagement_trends

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| int_engagement_theme_rollup | Direct pass-through | Theme scores by department and cycle with deltas |

**Purpose:** Engagement dashboard. Department-level only (anonymized data).

---

### fct_performance_distribution (RETAINED FROM V0)

| Source | Join Key | What It Provides |
|--------|----------|-----------------|
| stg_performance | employee_id | Rating per employee per cycle |
| stg_employees | employee_id | Current department, job_level, tenure for context |

**Purpose:** Per (employee x cycle) rating detail for the performance dashboard. Shows rating distribution, movement between cycles.

---

## Summary: Full Pipeline in One View

```
RAW LAYER (source system exports)
    raw_employees
    raw_performance
    raw_offers_hires
    raw_ees_responses
    raw_comp_bands
        │
        ▼
STAGING LAYER (clean, rename, filter, cast)
    stg_employees
    stg_performance
    stg_recruiting
    stg_engagement
    stg_comp_bands
        │
        ▼
INTERMEDIATE LAYER
    ┌─── Helper models (feed INTO roster) ───┐
    │  int_employee_tenure                    │
    │  int_employee_compensation_current      │
    │  int_employee_performance_history       │
    └─────────────────┬───────────────────────┘
                      │
                      ▼
              dim_calendar ──┐
                             │
              int_employee_monthly_roster (THE GOLDEN RECORD)
                      │
          ┌───────────┼────────────────────────┐
          │           │                        │
          ▼           ▼                        ▼
    Reporting Grids (scaffolds)          fct_employee_roster
    ├─ int_reporting_grid_attrition          (drill-through)
    ├─ int_reporting_grid_recruiting
    ├─ int_reporting_grid_workforce
    └─ int_reporting_grid_compensation
          │
          ▼
MART LAYER (Tableau reads these)
    fct_attrition_reporting
    fct_recruiting_reporting
    fct_workforce_composition
    fct_compensation_reporting

    Separate paths (no roster):
    stg_recruiting ──► int_recruiting_funnel_metrics ──► fct_recruiting_velocity
    stg_engagement ──► int_engagement_theme_rollup ──► fct_engagement_trends
    stg_performance + stg_employees ──► fct_performance_distribution
```
