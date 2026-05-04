# Project: JustKaizen AI - People Analytics Data Infrastructure (V1)

## Owner

Keenan Artis. Senior Data Analyst with 7+ years across forensics analytics (PwC) and people analytics (Attentive). Built Attentive's People Analytics reporting infrastructure from scratch: monthly employee roster, domain-specific reporting marts, TTM rolling calculations, and Tableau dashboards. This project recreates that architecture in dbt.

## Your Role

You are a Senior Data Engineer building a production-grade People Analytics data warehouse. The architecture is modeled after a real production system that served a ~950-person tech company. Write code as if a hiring manager will review every model, every test, and every commit message.

## What This Project Is

A People Analytics data warehouse for JustKaizen AI, a fictional pre-IPO enterprise AI company (1,200 active employees, ~1,900 total ever, remote-first). Demonstrates end-to-end infrastructure: synthetic data generation, dbt transformation layers (staging, intermediate, marts), BigQuery warehouse, automated Google Sheets delivery via Apps Script, and Tableau dashboards.

GitHub: https://github.com/keenanj-analytics/people-analytics-data-infrastructure

## Tech Stack

- Python (pandas) for synthetic data generation
- Google BigQuery (project: just-kaizen-ai)
- dbt for transformation (profile: justkaizen, ~/.dbt/profiles.yml)
- Google Sheets + Apps Script for data delivery
- Tableau Public for dashboards
- Git / GitHub for version control

## BigQuery Configuration

- GCP project: just-kaizen-ai
- Datasets: raw_raw (seeds), raw_staging (views), raw_intermediate (views), raw_marts (tables)
- dbt commands require: `export PATH="$HOME/Library/Python/3.9/bin:$PATH"`

---

## Architecture Overview

The system has one central table (int_employee_monthly_roster) that everything flows through. Think of it as a hub-and-spoke model.

```
Raw Sources → Staging (clean/rename) → Helper Intermediates → ROSTER → Reporting Grids → Marts
                                                                  └──→ fct_employee_roster (drill-through)

Separate paths (no roster):
stg_recruiting → int_recruiting_funnel_metrics → fct_recruiting_velocity
stg_engagement → int_engagement_theme_rollup → fct_engagement_trends
stg_performance + stg_employees → fct_performance_distribution
```

### Design Principles

1. **The employee monthly roster is the center.** One row per employee per month. All dimensions as columns. Every reporting mart aggregates from it. Every drill-through reads from it.
2. **Domain-specific reporting marts.** Each domain (attrition, recruiting, workforce, compensation) has its own mart with only relevant dimensions.
3. **Full time grid, no gaps.** dim_calendar + per-domain reporting grids ensure every month has a row, even with zero activity. Trend lines never break.
4. **Org-wide benchmarks on every row.** Each mart carries segment-level, department-level, and org-wide metrics for instant comparison.
5. **Row-based, not column-based.** Dimensions are rows, metrics are columns. Adding a new department never requires rebuilding a model.
6. **Pre-computed rolling windows.** TTM rates computed in SQL via `ROWS BETWEEN 11 PRECEDING AND CURRENT ROW`. Tableau computes zero rates.
7. **Excluded termination reasons are a data cleaning convention.** Applied at the roster level via `is_attrition_eligible_term`, not as dashboard filters.

---

## Key Reference Files

- `docs/JustKaizen_Data_Dictionary_V1.md` - Every field in every model with types, descriptions, examples
- `docs/JustKaizen_Model_Dependency_Map.md` - Every connection between models with join keys and logic
- `docs/JustKaizen_Company_Profile_Definitive.md` - Company story, org structure, headcount, comp bands, attrition targets
- `docs/JustKaizen_dbt_Architecture_Spec_v2.md` - Full model specifications

---

## Raw Source Tables (5 seeds)

| Seed | Source System | Description | Est. Rows |
|------|-------------|-------------|-----------|
| raw_employees | ADP (HRIS) | Combined active + terminated employee census. One row per employee per Report_Date snapshot. PK: Work_Email. | ~65,000+ |
| raw_performance | Lattice | Performance reviews. One row per (Question_ID x Reviewee_Email x Cycle_Name). Includes manager, self, peer reviews and multiple question types. | ~15,000+ |
| raw_offers_hires | ATS | Recruiting pipeline. One row per candidate-job combination. All candidates (hired, rejected, declined). PK: Candidate_ID. | ~30,000+ |
| raw_ees_responses | Survey Platform | Employee engagement surveys. Anonymized individual responses. PK: Anonymized_User_ID per question per cycle. | ~25,000+ |
| raw_comp_bands | Total Rewards | Compensation band structure. One row per job title. Zone A and Zone B bands. PK: Title. | ~200+ |

---

## Model Inventory (29 models)

### Staging Layer (6 views)

| Model | Source | Key Transformation |
|-------|--------|-------------------|
| stg_employees | raw_employees | Rename fields to snake_case, cast dates, map Manager_Email to manager_id |
| stg_performance | raw_performance | **FILTER:** Response_Type = 'manager' AND Question = 'Performance Category'. Invert rating scale (source 1=best → target 5=best). Output: one row per employee per cycle. |
| stg_recruiting | raw_offers_hires | Rename verbose fields, add employee_id for hired candidates, compute time_to_fill_days and time_to_hire_days, rename Source to application_channel |
| stg_engagement | raw_ees_responses | Rename fields, rename Radford_Level to job_level |
| stg_comp_bands | raw_comp_bands | Parse salary strings ("$174,000" → 174000.00), rename Zone A/B fields |
| stg_job_history | raw_job_history (if separate) | Clean rename. May be derived from monthly employee snapshots instead. |

**Staging rules:** No joins between tables. No business logic. No aggregation. No derived fields. Just clean, rename, cast, filter.

### Intermediate Layer (12 views)

**Helper models (feed into the roster):**

| Model | Sources | Output |
|-------|---------|--------|
| int_employee_tenure | stg_employees, stg_job_history | One row per employee: tenure_months, total_promotions, career_velocity |
| int_employee_compensation_current | stg_employees, stg_comp_bands | One row per employee: latest salary, matched comp band (Zone A or B based on Pay_Zone), compa_ratio |
| int_employee_performance_history | stg_performance | One row per employee: latest_perf_rating, latest_perf_rating_numeric, top_performer_flag |

**The golden record:**

| Model | Sources | Output |
|-------|---------|--------|
| int_employee_monthly_roster | stg_employees, dim_calendar, int_employee_tenure, int_employee_compensation_current, int_employee_performance_history, stg_recruiting | One row per employee per month. ~65,000-75,000 rows. ALL dimensions as columns. |

**Reporting grids (scaffolds, one per domain):**

| Model | Grain |
|-------|-------|
| int_reporting_grid_attrition | report_month x department x sub_department x job_level x level_group x tenure_bucket x top_performer_flag x gender x race_ethnicity |
| int_reporting_grid_recruiting | report_month x department x sub_department x job_level x candidate_source x candidate_origin x candidate_recruiter x candidate_hiring_manager |
| int_reporting_grid_workforce | report_month x department x sub_department x job_level x level_group x gender x race_ethnicity x manager_status |
| int_reporting_grid_compensation | report_month x department x sub_department x job_level x level_group x gender x latest_performance_rating |

**Separate paths (no roster involvement):**

| Model | Source | Output |
|-------|--------|--------|
| int_recruiting_funnel_metrics | stg_recruiting | One row per requisition: funnel volumes, conversion rates, time_to_fill |
| int_engagement_theme_rollup | stg_engagement | Aggregated to (cycle x department x theme): theme_avg_score, enps_score, deltas |

**Standalone:**

| Model | Output |
|-------|--------|
| dim_calendar | One row per day, Jan 1 2020 through Dec 31 2026. Generated via GENERATE_DATE_ARRAY. |

### Mart Layer (11 tables)

**Domain reporting marts (aggregated from roster via grids):**

| Model | Grain | Key Metrics |
|-------|-------|-------------|
| fct_attrition_reporting | report_month x dimension combo | end_month_headcount, termination counts (total, voluntary, involuntary, top performer, regrettable), TTM rates, org-wide and dept benchmarks |
| fct_recruiting_reporting | report_month x dimension combo | total_hires, offers extended/accepted/declined, time_to_fill, TTM rates, org-wide benchmarks |
| fct_workforce_composition | report_month x dimension combo | headcount, hires, terminations, net_change, representation rates, avg_span_of_control, avg_tenure |
| fct_compensation_reporting | report_month x dimension combo | avg_salary, avg_compa_ratio, avg_band_position, count_below/above_band, median_compa_ratio, benchmarks |

**Drill-through and detail marts:**

| Model | Source | Purpose |
|-------|--------|---------|
| fct_employee_roster | int_employee_monthly_roster (promoted) | Drill-through. "Who left?" "Who is below band?" Filter this table. |
| fct_recruiting_velocity | int_recruiting_funnel_metrics | Per-requisition detail for hiring drill-through |
| fct_engagement_trends | int_engagement_theme_rollup | Theme scores by department and cycle |
| fct_performance_distribution | stg_performance + stg_employees | Per (employee x cycle) ratings with org context |

---

## Build Order

Build in this exact sequence. Each model depends on the ones above it.

```
1.  dim_calendar                          # no dependencies
2.  stg_employees                         # raw_employees
3.  stg_performance                       # raw_performance
4.  stg_recruiting                        # raw_offers_hires
5.  stg_engagement                        # raw_ees_responses
6.  stg_comp_bands                        # raw_comp_bands
7.  stg_job_history                       # raw_job_history (if separate)
8.  int_employee_tenure                   # stg_employees + stg_job_history
9.  int_employee_compensation_current     # stg_employees + stg_comp_bands
10. int_employee_performance_history      # stg_performance
11. int_employee_monthly_roster           # dim_calendar + stg_employees + helpers + stg_recruiting
12. int_reporting_grid_attrition          # dim_calendar + roster
13. int_reporting_grid_recruiting         # dim_calendar + roster
14. int_reporting_grid_workforce          # dim_calendar + roster
15. int_reporting_grid_compensation       # dim_calendar + roster
16. fct_attrition_reporting               # grid_attrition + roster
17. fct_recruiting_reporting              # grid_recruiting + stg_recruiting
18. fct_workforce_composition             # grid_workforce + roster
19. fct_compensation_reporting            # grid_compensation + roster
20. fct_employee_roster                   # roster (promoted)
21. int_recruiting_funnel_metrics         # stg_recruiting
22. int_engagement_theme_rollup           # stg_engagement
23. fct_recruiting_velocity               # int_recruiting_funnel_metrics
24. fct_engagement_trends                 # int_engagement_theme_rollup
25. fct_performance_distribution          # stg_performance + stg_employees
```

After each model: `dbt run --select model_name`, then `dbt test --select model_name`. Fix before moving to next.

---

## Level Framework

| Level | Level Group |
|-------|-------------|
| P1-P3 | Junior IC |
| P4-P6 | Senior IC |
| M1-M2 | Manager |
| M3-M4 | Director |
| E1-E6 | Senior Leadership |

No M5 or M6 levels exist. VPs are E1, SVPs are E2, C-Suite is E3-E4, CEO is E6.

## Tenure Buckets

1-year intervals. Boundary: exactly 12 months = "1-2 Years" (exclusive lower, inclusive upper).

| Bucket | Range |
|--------|-------|
| 0-1 Years | 0 < tenure_months <= 12 |
| 1-2 Years | 12 < tenure_months <= 24 |
| 2-3 Years | 24 < tenure_months <= 36 |
| 3-4 Years | 36 < tenure_months <= 48 |
| 4-5 Years | 48 < tenure_months <= 60 |
| 5+ Years | tenure_months > 60 |

## Performance Rating Scale

Target uses 1-5 (5 = best). Source (Lattice) uses 1-4 (1 = best). Staging inverts.

| Target Numeric | Target Description | Source Score |
|---------------|-------------------|-------------|
| 5 | Significantly Exceeds Expectations | 1 |
| 4 | Exceeds Expectations | 2 |
| 3 | Meets Expectations | 3 |
| 2 | Partially Meets Expectations | N/A (JustKaizen addition) |
| 1 | Does Not Meet Expectations | 4 |

## Top Performer Flag

"Y" if latest_perf_rating_numeric >= 4 OR is_critical_talent = TRUE. Otherwise "N".

## Excluded Termination Reasons

Data cleaning convention. These are structural/operational changes, not true attrition. Applied via `is_attrition_eligible_term` on the roster.

- Reduction in Force
- End of Contract
- Entity Change
- Acquisition/Merger
- End of Internship
- International Transfer
- Relocation (involuntary, company-initiated)
- Converting to FT

## TTM Attrition Rate Formula

```sql
TTM Attrition Rate =
  SUM(terminations) OVER (ORDER BY report_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
  /
  AVG(end_month_headcount) OVER (ORDER BY report_month ROWS BETWEEN 11 PRECEDING AND CURRENT ROW)
```

Denominator is the average of 12 monthly end headcounts, not a two-point average. This smooths RIF distortion.

## Compa-Ratio Formula

```sql
compa_ratio = ROUND(salary / comp_band_mid, 2)
```

Band matched by job_title. Zone A or Zone B selected based on employee's Pay_Zone.

---

## V0 → V1 Migration

### Models to REMOVE (replaced by V1 architecture)

- fct_monthly_metrics (replaced by 4 domain-specific reporting marts)
- int_monthly_headcount_snapshot (replaced by int_employee_monthly_roster)
- int_employee_dimension (absorbed into roster)
- int_employee_event_sequence (not needed)
- fct_workforce_overview (replaced by fct_workforce_composition)
- fct_attrition_drivers (replaced by fct_employee_roster filtered to terminated)
- fct_compensation_parity (replaced by fct_compensation_reporting + fct_employee_roster)

### Models to KEEP (no changes needed)

- All 6 staging models (stg_employees, stg_job_history, stg_compensation, stg_performance, stg_recruiting, stg_engagement)
- int_employee_tenure
- int_employee_compensation_current
- int_employee_performance_history
- int_recruiting_funnel_metrics
- int_engagement_theme_rollup
- fct_recruiting_velocity
- fct_engagement_trends
- fct_performance_distribution

---

## Code Style

- SQL: BigQuery dialect. Lowercase keywords. snake_case column names. One column per line in SELECT.
- CTE pattern: `WITH source AS (...), renamed AS (...), final AS (...) SELECT * FROM final`
- Every model starts with a block comment: purpose, source tables, grain, key business rules.
- dbt refs: always use `{{ ref('model_name') }}`, never hardcoded table names.
- Naming: staging = `stg_`, intermediate = `int_`, marts = `fct_`, dimension = `dim_`.
- Materialization: staging = view, intermediate = view, marts = table, dim_calendar = table.
- Tests: primary key uniqueness, not_null on PKs, accepted_values for categorical fields, relationships between tables.
- Commit messages: descriptive. "Add fct_attrition_reporting with TTM rolling windows and org-wide benchmarks" not "add model".

## Terminal Commands

```bash
export PATH="$HOME/Library/Python/3.9/bin:$PATH"  # Required before dbt
dbt deps                    # Install packages
dbt seed                    # Load CSVs into BigQuery
dbt run                     # Build all models
dbt test                    # Run all tests
dbt run --select model_name           # Build one model
dbt run --select model_name+          # Build model + everything downstream
dbt test --select model_name          # Test one model
git add .
git commit -m "descriptive message"
git push
```
