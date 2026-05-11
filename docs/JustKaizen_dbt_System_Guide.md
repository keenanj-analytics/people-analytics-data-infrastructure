# JustKaizen AI — People Analytics Data Infrastructure

## dbt System Guide

**Combined Reference & Workflow Guide**

Version 1.0 — May 2026

25 dbt models · 6 CSV seeds · 124 tests · 628,034 records

---

## Table of Contents

- [Part 1: System Overview](#part-1-system-overview)
- [Part 2: What Each Layer Does](#part-2-what-each-layer-does)
  - [Layer 1 — Setup & Configuration](#layer-1--setup--configuration)
  - [Layer 2 — Source Data (Seeds)](#layer-2--source-data-seeds)
  - [Layer 3 — Staging Models](#layer-3--staging-models)
  - [Layer 4 — Intermediate Models](#layer-4--intermediate-models)
  - [Layer 5 — Mart Models (Reporting Tables)](#layer-5--mart-models-reporting-tables)
  - [Layer 6 — Testing & Validation](#layer-6--testing--validation)
  - [Layer 7 — Export Pipeline](#layer-7--export-pipeline)
  - [Layer 8 — Visualization (Tableau)](#layer-8--visualization-tableau)
- [Part 3: dbt Commands Reference](#part-3-dbt-commands-reference)
- [Part 4: How to Do Things](#part-4-how-to-do-things)
- [Part 5: Troubleshooting](#part-5-troubleshooting)
- [Part 6: System Flow Diagram](#part-6-system-flow-diagram)
- [Part 7: Evolution Recommendations](#part-7-evolution-recommendations)

---

## Part 1: System Overview

The JustKaizen AI People Analytics Data Infrastructure is a production-grade dbt project that transforms raw HR data into reporting-ready fact tables for Tableau dashboards. The system models a fictional 1,200-person tech company (1,900 unique employees all-time including terminations) across five analytics domains: attrition, workforce composition, recruiting, compensation, and engagement.

**Architecture at a glance:**

- 6 CSV seed files (raw HRIS, ATS, performance, engagement, comp bands, job history)
- 6 staging models — clean, rename, type-cast raw data
- 11 intermediate models — golden record roster, reporting grids, helper tables
- 8 mart models — fact tables for Tableau dashboards
- 124 schema tests across all layers
- 628,034 total records across 8 mart tables

**Data flow:**

```
CSV Seeds → dbt seed → BigQuery (raw_raw)
  → dbt run → Staging (raw_staging) → Intermediate (raw_intermediate) → Marts (raw_marts)
    → Python export → CSVs → Google Drive → Tableau Public
```

**Tech stack:**

- dbt Core (CLI) — transformation framework
- BigQuery — cloud data warehouse
- Python — CSV export script
- Google Drive — CSV hosting for Tableau
- Tableau Public — visualization layer

---

## Part 2: What Each Layer Does

### Layer 1 — Setup & Configuration

#### dbt Installation

dbt Core is installed locally via pip. It runs from the command line against BigQuery.

```bash
pip3 install dbt-bigquery
```

This installs dbt Core + the BigQuery adapter in one package.

#### profiles.yml

*Location: ~/.dbt/profiles.yml*

This file lives OUTSIDE your project directory (in your home folder). It contains your BigQuery connection credentials. dbt reads it every time you run a command.

```yaml
justkaizen:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: just-kaizen-ai
      dataset: raw
      keyfile: ~/Documents/Claude_Code/Keys/just-kaizen-ai-6ee503c7c428.json
      threads: 4
```

**Key fields:** `project` = your GCP project ID. `dataset` = the default BigQuery dataset (dbt appends schema overrides to this, e.g., raw + _staging = raw_staging). `keyfile` = path to your service account JSON key. `threads` = parallel model builds.

#### dbt_project.yml

*Location: project root*

The project configuration file. Defines the project name, profile to use, file paths, and materialization/schema settings per layer.

```yaml
name: 'justkaizen_analytics'
profile: 'justkaizen'

models:
  justkaizen_analytics:
    staging:
      +materialized: view
      +schema: staging          # → raw_staging
    intermediate:
      +materialized: view
      +schema: intermediate     # → raw_intermediate
      dim_calendar:
        +materialized: table    # override: table
    marts:
      +materialized: table
      +schema: marts            # → raw_marts

seeds:
  justkaizen_analytics:
    +schema: raw                # → raw_raw
```

**Materialization:** `view` = SQL view in BigQuery (no storage cost, recomputes on query). `table` = physical table (stored, faster reads). Staging and intermediate use views by default. Marts are tables. dim_calendar is a table override because every reporting grid reads from it.

#### BigQuery Datasets

dbt creates four datasets in BigQuery, one per layer:

| Dataset | Layer | Materialization | Contents |
|---------|-------|-----------------|----------|
| raw_raw | Seeds | Tables (from CSV) | 6 raw source tables |
| raw_staging | Staging | Views | 6 stg_ models |
| raw_intermediate | Intermediate | Views + 1 table | 11 int_ models + dim_calendar |
| raw_marts | Marts | Tables | 8 fct_ models |

---

### Layer 2 — Source Data (Seeds)

Seeds are CSV files in the `seeds/` directory. Running `dbt seed` loads them into BigQuery as tables in the raw_raw dataset. They represent source system exports.

| Seed File | What It Is | Grain | Key Columns |
|-----------|-----------|-------|-------------|
| raw_employees.csv | HRIS export — active + terminated employees | 1 row per employee per report_date | employee_id, hire_date, termination_date, department, job_level, salary |
| raw_performance.csv | Performance management — all review types | 1 row per question × reviewee × cycle | Reviewee_Email, Cycle_Name, Overall_Rating |
| raw_offers_hires.csv | ATS recruiting pipeline | 1 row per candidate-job combo | Candidate_ID, Requisition_ID, Outcome, Source |
| raw_ees_responses.csv | Engagement survey (anonymized) | 1 row per user × question × cycle | Anonymized_User_ID, EES_Theme_Name, Response_Likert |
| raw_comp_bands.csv | Compensation band structure | 1 row per job title | Title, Zone_A_Min, Zone_A_Mid, Zone_A_Max |
| raw_job_history.csv | Job change events | 1 row per change event | Employee_ID, Effective_Date, Change_Type |

**Important:** raw_ees_responses.csv is the largest seed (~165K rows, ~27 seconds to load). When running `dbt build`, tests on stg_engagement can fire before BigQuery has finished writing this seed. **Fix: run `dbt seed` first, then `dbt build` separately.**

#### sources.yml

*Location: models/staging/sources.yml*

Declares the seed tables as dbt sources so staging models can reference them with `{{ source('raw', 'raw_employees') }}` instead of hardcoded table names.

---

### Layer 3 — Staging Models

Staging models clean and rename raw source data. The pattern is consistent: rename columns to snake_case, cast dates, and apply minimal filtering. No aggregation or joins (with one exception: stg_recruiting resolves employee_id for hires).

| Model | Source | Grain | What It Does |
|-------|--------|-------|-------------|
| stg_employees | raw_employees | 1 row per employee per report_date | Rename columns, cast Hire_Date and Termination_Date to DATE, surface canonical IDs (employee_id = work_email, manager_id) |
| stg_performance | raw_performance | 1 row per employee per cycle | Filter to manager ratings on "Performance Category" question only. Invert 1-5 source scale to target 5-point (1→5, 2→4, 3→3, 4→2, 5→1) |
| stg_recruiting | raw_offers_hires | 1 row per candidate | Rename ATS fields, compute time_to_fill_days and time_to_hire_days, LEFT JOIN to stg_employees to resolve employee_id for hired candidates |
| stg_engagement | raw_ees_responses | 1 row per user × question × cycle | Rename columns, cast submission date. Anonymized — no link to employee roster |
| stg_comp_bands | raw_comp_bands | 1 row per job title | Parse salary strings ("$174,000" → 174000.0 as FLOAT64), rename to snake_case |
| stg_job_history | raw_job_history | 1 row per change event | Rename columns, cast effective_date to DATE. Uses old_X / new_X column convention |

**Schema tests** are defined in `models/staging/schema.yml`. Tests include: not_null, unique (on PKs), accepted_values (e.g., employment_status must be 'Active' or 'Terminated'). Integer columns like overall_rating_numeric use `quote: false` to prevent BigQuery type mismatch (STRING vs INT64).

---

### Layer 4 — Intermediate Models

The intermediate layer has three types of models: helper tables (tenure, comp, performance), reporting grids (scaffolds), and the golden record (int_employee_monthly_roster). Plus dim_calendar, the date spine that anchors everything.

#### dim_calendar — Date spine (the foundation)

One row per calendar day from 2021-01-01 through 2026-03-31 (~1,917 rows). Generated entirely via BigQuery's GENERATE_DATE_ARRAY — no upstream dependencies. Reporting grids CROSS JOIN distinct report_months from this table to guarantee a row in every trend line for every month, even months with zero activity. **Materialized as table** (the only intermediate model that is).

**Key columns:** calendar_date, report_month, report_quarter, report_year, is_month_end, is_quarter_end, flag_latest_month

#### Helper Tables — Per-employee enrichment

| Model | Grain | What It Computes |
|-------|-------|-----------------|
| int_employee_tenure | 1 row per employee | tenure_months, total_promotions (from stg_job_history), career_velocity_per_year |
| int_employee_compensation_current | 1 row per employee | Latest salary matched to comp band (Zone A or B by employee_zone), compa_ratio = salary / band_mid |
| int_employee_performance_history | 1 row per employee | Latest completed manager review, top_performer_flag (rating >= 4) |

#### int_employee_monthly_roster — The Golden Record ★

THE center of the warehouse. One row per employee per month they were active (or terminated in that month). 46 columns. All four domain reporting marts aggregate from this table.

**Construction:**

- CROSS JOIN deduplicated stg_employees (latest snapshot per employee) with distinct report_months from dim_calendar
- Filter: employee appears in a month if `hire_date <= last day of month` AND (`termination_date IS NULL` OR `termination_date >= start of month`)
- LEFT JOIN int_employee_compensation_current, int_employee_performance_history, and deduplicated stg_recruiting (latest hired record per employee)

**Derived fields:** level_group, tenure_months, tenure_bucket, new_hire_flag, employment_status, termination_type, top_performer_flag, flag_latest_report, is_terminated_this_month, is_excluded_termination, is_attrition_eligible_term, is_rif_termination

**V1 design decision:** Static (latest-known) values for compensation, performance, and all employee attributes. Phase 2 introduces per-month point-in-time joins.

#### Reporting Grids — Scaffold tables for zero-fill trend continuity

Four reporting grids, one per domain. Each CROSS JOINs distinct report_months from dim_calendar with distinct dimension combinations from int_employee_monthly_roster. This guarantees a row in every trend line for every month — even months with zero activity (no hires, no terms, etc.).

| Grid | Domain | Dimensions Cross-Joined |
|------|--------|------------------------|
| int_reporting_grid_attrition | Attrition | department, sub_department, job_level, level_group, tenure_bucket, top_performer_flag, gender, race_ethnicity |
| int_reporting_grid_workforce | Workforce | department, sub_department, job_level, level_group, gender, race_ethnicity, manager_status |
| int_reporting_grid_recruiting | Recruiting | department, sub_department, job_level, candidate_source, candidate_origin, candidate_recruiter, candidate_hiring_manager |
| int_reporting_grid_compensation | Compensation | department, sub_department, job_level, level_group, gender, latest_perf_rating |

#### Other Intermediate Models

- **int_recruiting_funnel_metrics** — Per-requisition funnel rollup: total_applicants → screened → interviewed → offered → hired → declined. Stage-to-stage conversion rates, time_to_fill_days, offer_acceptance_rate, top_source. Grain: 1 row per requisition_id.
- **int_engagement_theme_rollup** — Engagement scores aggregated to survey_cycle × department × theme. Computes theme_avg_score, favorable_pct (likert >= 4), eNPS (100 × (Promoters − Detractors) / total), and cycle-over-cycle deltas via LAG().

---

### Layer 5 — Mart Models (Reporting Tables)

8 fact tables materialized as BigQuery tables in raw_marts. These are what Tableau reads. Four are domain reporting tables (aggregated, with TTM rolling windows and benchmarks). Two are drill-through tables. Two are standalone.

#### Domain Reporting Tables — Aggregated with TTM and benchmarks

| Model | Grain | Rows | Cols | Key Metrics |
|-------|-------|------|------|-------------|
| fct_attrition_reporting | month × 8 dimensions | 274,869 | 28 | Headcount, total/voluntary/involuntary/top-performer/regrettable terminations, TTM attrition rates, org-wide and dept benchmarks |
| fct_workforce_composition | month × 8 dimensions | 89,964 | 22 | Headcount, hires, terminations, net_change, representation rates, avg span of control, avg tenure months |
| fct_recruiting_reporting | month × 7 dimensions | 119,133 | 24 | Hires, offers extended/accepted/declined, avg time to fill, TTM offer acceptance rate, org benchmarks |
| fct_compensation_reporting | month × 7 dimensions | 79,191 | 20 | Avg salary, avg compa-ratio, avg band position, count below/above band, median compa-ratio, org/dept benchmarks |

**TTM (Trailing Twelve Months):** Attrition and recruiting use 12-month rolling windows via `ROWS BETWEEN 11 PRECEDING AND CURRENT ROW`. Denominators are averaged (not two-point). Workforce and compensation are point-in-time (no TTM). A 3-month rolling window (annualized via * 4) provides quarterly attrition performance alongside TTM.

**Termination classification:** is_attrition_eligible_term excludes: Reduction in Force, End of Contract, Entity Change, Acquisition/Merger, End of Internship, International Transfer, Relocation, Converting to FT. is_rif_termination flags RIF-specific terminations separately. Involuntary attrition-eligible reasons include Performance, Misconduct, and Policy Violation. fct_workforce_composition counts ALL terminations; fct_attrition_reporting uses the eligibility flag and carries rif_terminations and total_terminations_plus_rif for complete separation visibility.

#### Drill-Through & Standalone Tables

| Model | Grain | Rows | Cols | What It Is |
|-------|-------|------|------|-----------|
| fct_employee_roster | 1 row per employee per month | 53,233 | 48 | Full roster promoted from int_employee_monthly_roster + is_active and band_position_label. Drill-through for all dashboards. |
| fct_recruiting_velocity | 1 row per requisition | 1,900 | 18 | Per-requisition funnel detail. Promoted from int_recruiting_funnel_metrics. |
| fct_engagement_trends | 1 row per cycle × dept × theme | 720 | 11 | Theme scores with cycle-over-cycle deltas and eNPS. Promoted from int_engagement_theme_rollup. |
| fct_performance_distribution | 1 row per employee × cycle | 9,024 | 11 | Per-employee per-cycle ratings with org context. Drives performance distribution dashboard. |

---

### Layer 6 — Testing & Validation

124 schema tests defined across three schema.yml files (staging, intermediate, marts). Tests run automatically as part of `dbt build` or manually via `dbt test`.

**Test types used:**

- **not_null** — Column must have no NULL values
- **unique** — Column must have no duplicate values (used on primary keys)
- **accepted_values** — Column values must be from a defined set (e.g., employment_status IN ('Active', 'Terminated'))

**Known issue — `quote: false`**

dbt's accepted_values test quotes values by default (`quote: true`). For integer columns like overall_rating_numeric, this causes a BigQuery type mismatch: the test compiles to `IN ('1', '2', '3', '4', '5')` but the column is INT64. **Fix:** Add `quote: false` to the test definition.

```yaml
- name: overall_rating_numeric
  tests:
    - accepted_values:
        values: [1, 2, 3, 4, 5]
        quote: false
```

---

### Layer 7 — Export Pipeline

A Python script exports all 8 mart tables from BigQuery to individual CSV files. CSVs are uploaded to Google Drive for Tableau to consume.

#### scripts/export_marts_to_csv.py

**What it does:** Connects to BigQuery using the same service account key as dbt. Runs SELECT * on each mart table. Writes results to `exports/` as individual CSVs.

```bash
pip3 install google-cloud-bigquery pandas db-dtypes
python3 scripts/export_marts_to_csv.py
```

**Output:** 8 CSV files in `exports/` directory, totaling 628,034 rows.

**Why CSVs instead of Google Sheets?**

- Google Sheets has a 10M cell limit per spreadsheet. fct_attrition_reporting alone (274K rows × 28 cols) = 7.7M cells.
- CSVs have no row/column limits. Full SELECT * with all columns, all rows, all months.
- CSVs are version-controllable and don't depend on Apps Script middleware.

---

### Layer 8 — Visualization (Tableau)

Tableau Public connects to the CSV files hosted on Google Drive. Each CSV becomes a separate data source in Tableau. Dashboards span five analytics domains:

- **Attrition Dashboard** — fct_attrition_reporting (TTM rates, voluntary/involuntary splits, top performer retention)
- **Workforce Dashboard** — fct_workforce_composition (headcount trends, representation, span of control)
- **Recruiting Dashboard** — fct_recruiting_reporting + fct_recruiting_velocity (pipeline health, time to fill, offer acceptance)
- **Compensation Dashboard** — fct_compensation_reporting (compa-ratio distribution, band position, pay equity)
- **Engagement Dashboard** — fct_engagement_trends (theme scores, eNPS, cycle-over-cycle deltas)
- **Performance Dashboard** — fct_performance_distribution (rating distribution, top performer identification)
- **Drill-through** — fct_employee_roster (employee-level detail for any dashboard)

---

## Part 3: dbt Commands Reference

Run all commands from your project root directory.

| Command | What It Does | When to Use |
|---------|-------------|-------------|
| `dbt seed` | Loads CSV files from seeds/ into BigQuery as tables in raw_raw | First-time setup, or when seed data changes. Run BEFORE dbt build to avoid timing issues with large seeds. |
| `dbt run` | Builds all models (staging → intermediate → marts) in dependency order | After changing any .sql model file. Creates views/tables in BigQuery. |
| `dbt test` | Runs all schema tests defined in schema.yml files | After dbt run to validate data quality. Can target specific models: `dbt test -s stg_engagement` |
| `dbt build` | Runs seed + run + test in dependency order | The "do everything" command. Can timeout on large seeds — prefer separate seed + build. |
| `dbt compile` | Compiles Jinja SQL to raw SQL without executing. Output in target/compiled/ | Debugging — see the exact SQL dbt will send to BigQuery. |
| `dbt build -s model_name+` | Builds a specific model and everything downstream of it | After changing one model. The `+` means "and all dependents." |
| `dbt build -s +model_name` | Builds everything upstream of a model, then the model itself | Rebuilding a model with all its dependencies. |
| `dbt debug` | Tests your connection to BigQuery and validates profiles.yml | First-time setup or when you get auth errors. |
| `dbt deps` | Installs packages from packages.yml (e.g., dbt_utils) | First-time setup or after adding a new package. |
| `dbt clean` | Deletes target/ and dbt_packages/ directories | When you want a fresh build from scratch. |

---

## Part 4: How to Do Things

### A. First-Time Setup (from scratch)

1. Install dbt: `pip3 install dbt-bigquery`
2. Clone the repo: `git clone https://github.com/keenanj-analytics/people-analytics-data-infrastructure.git`
3. Create `~/.dbt/profiles.yml` with your BigQuery service account key (see Layer 1)
4. Test connection: `dbt debug`
5. Install packages: `dbt deps`
6. Load seeds: `dbt seed` (wait for completion — large seeds take ~30s)
7. Build everything: `dbt build`
8. Verify: all 124 tests should pass

### B. Rebuild After Model Changes

1. Edit the .sql file in models/
2. Run: `dbt build -s changed_model_name+` (builds the model + everything downstream)
3. Check test results in the output

### C. Refresh Data for Tableau

1. Run: `dbt build` (rebuilds all models in BigQuery)
2. Run: `python3 scripts/export_marts_to_csv.py` (exports fresh CSVs)
3. Upload CSVs from exports/ to Google Drive
4. Tableau reads updated data on next extract refresh

### D. Add a New Model

1. Create a new .sql file in the appropriate models/ subdirectory
2. Reference upstream models with `{{ ref('model_name') }}`
3. Add tests in the layer's schema.yml file
4. Run: `dbt build -s new_model_name+`

### E. Update Seed Data

1. Replace the CSV file in seeds/ with updated data
2. Run: `dbt seed` (reloads the table in BigQuery)
3. Run: `dbt build` (rebuilds all downstream models)

### F. Check What SQL dbt Will Run

1. Run: `dbt compile`
2. Open `target/compiled/justkaizen_analytics/models/` to see the compiled SQL
3. You can paste this SQL directly into the BigQuery console to test

---

## Part 5: Troubleshooting

### FIX: "Your default credentials were not found"

This means Python can't find BigQuery auth.

- Check that your profiles.yml has the correct keyfile path
- Verify the JSON key file exists at that path
- If running the Python export script, it reads the keyfile path from the script itself — update KEYFILE in export_marts_to_csv.py

### FIX: Tests fail with "accepted_values" type mismatch

BigQuery error: IN operator with STRING vs INT64.

- **Cause:** dbt quotes values by default. Integer columns need `quote: false`
- **Fix:** Add `quote: false` to the accepted_values test in schema.yml
- **Affected columns:** overall_rating_numeric (staging + marts), latest_perf_rating_numeric (intermediate)

### FIX: Engagement tests fail with "raw_ees_responses not found"

Race condition. The large engagement seed (~165K rows) takes ~27 seconds to load.

- **Cause:** `dbt build` runs seeds and tests concurrently. Tests fire before BigQuery finishes writing the seed.
- **Fix:** Run `dbt seed` first, wait for completion, then run `dbt build`

### FIX: Flat headcount from a certain month onward

Active employees with NULL termination_date are cross-joined into every future month.

- **Cause:** dim_calendar generates dates beyond the scope of your data
- **Fix:** Cap dim_calendar's GENERATE_DATE_ARRAY end date to match your data scope (currently `DATE '2026-03-31'`)
- **Location:** models/intermediate/dim_calendar.sql, line with `generate_date_array()`

### FIX: dbt debug fails

- Check `~/.dbt/profiles.yml` exists and has correct indentation (YAML is whitespace-sensitive)
- Verify the profile name in profiles.yml matches the profile field in dbt_project.yml (`justkaizen`)
- Verify the service account JSON key has BigQuery permissions

### FIX: "Compilation Error — model not found"

- Check that the model name in `{{ ref('model_name') }}` exactly matches the .sql filename (without .sql)
- Run `dbt deps` if the model is from an external package

---

## Part 6: System Flow Diagram

Use this as a reference for building a Lucidchart or similar diagram.

```
SEEDS (CSV files)
├── raw_employees.csv
├── raw_performance.csv
├── raw_offers_hires.csv
├── raw_ees_responses.csv
├── raw_comp_bands.csv
└── raw_job_history.csv
        │
    dbt seed
        │
        ▼
RAW (BigQuery: raw_raw)
├── raw_employees
├── raw_performance
├── raw_offers_hires
├── raw_ees_responses
├── raw_comp_bands
└── raw_job_history
        │
    dbt run
        │
        ▼
STAGING (BigQuery: raw_staging — views)
├── stg_employees
├── stg_performance
├── stg_recruiting ←── stg_employees (resolves employee_id)
├── stg_engagement
├── stg_comp_bands
└── stg_job_history
        │
        ▼
INTERMEDIATE (BigQuery: raw_intermediate — views + 1 table)
│
├── dim_calendar ◄── GENERATE_DATE_ARRAY (no dependencies) [TABLE]
│
├── HELPER TABLES
│   ├── int_employee_tenure ←── stg_employees + stg_job_history
│   ├── int_employee_compensation_current ←── stg_employees + stg_comp_bands
│   └── int_employee_performance_history ←── stg_performance
│
├── GOLDEN RECORD ★
│   └── int_employee_monthly_roster ←── all helpers + dim_calendar + stg_recruiting
│
├── REPORTING GRIDS (CROSS JOIN dim_calendar × roster dimensions)
│   ├── int_reporting_grid_attrition
│   ├── int_reporting_grid_workforce
│   ├── int_reporting_grid_recruiting
│   └── int_reporting_grid_compensation
│
├── int_recruiting_funnel_metrics ←── stg_recruiting
└── int_engagement_theme_rollup ←── stg_engagement
        │
        ▼
MARTS (BigQuery: raw_marts — tables)
│
├── DOMAIN REPORTING (aggregated + scaffolded + TTM + benchmarks)
│   ├── fct_attrition_reporting ←── roster + grid     274,869 rows × 28 cols
│   ├── fct_workforce_composition ←── roster + grid    89,964 rows × 22 cols
│   ├── fct_recruiting_reporting ←── roster + grid    119,133 rows × 24 cols
│   └── fct_compensation_reporting ←── roster + grid   79,191 rows × 20 cols
│
├── DRILL-THROUGH
│   ├── fct_employee_roster ←── roster (promoted)      53,233 rows × 48 cols
│   └── fct_recruiting_velocity ←── funnel (promoted)   1,900 rows × 18 cols
│
└── STANDALONE
    ├── fct_engagement_trends ←── rollup (promoted)       720 rows × 11 cols
    └── fct_performance_distribution ←── stg_perf + emp 9,024 rows × 11 cols
        │
    Python export script
        │
        ▼
EXPORT (exports/ — CSVs)
├── fct_attrition_reporting.csv
├── fct_workforce_composition.csv
├── ...all 8 tables...
        │
    Upload to Drive
        │
        ▼
GOOGLE DRIVE (CSV hosting)
        │
    Tableau connects
        │
        ▼
TABLEAU PUBLIC (dashboards)
├── Attrition Dashboard
├── Workforce Dashboard
├── Recruiting Dashboard
├── Compensation Dashboard
├── Engagement Dashboard
├── Performance Dashboard
└── Employee Drill-Through
```

---

## Part 7: Evolution Recommendations

Recommendations for evolving the system from V1 to production-grade, ordered by impact.

### 1. Point-in-Time Joins (High Impact)

V1 uses static latest-known values for compensation, performance, and all employee attributes. This means a department transfer in January shows the new department in every historical month. Phase 2 should introduce per-month point-in-time joins using SCD Type 2 logic or monthly snapshots.

### 2. Single Wide Table for Tableau (High Impact)

Create a single pre-joined fact table (e.g., fct_workforce_detail) at the employee × month grain with all workforce, compensation, and attrition metrics baked in. Eliminates the need for Tableau joins/blending and gives Tableau one clean source per dashboard.

### 3. Incremental Models (Medium Impact)

The four domain reporting marts rebuild from scratch every run (~628K rows). Switch to dbt incremental materialization to only process new/changed data. Reduces build time and BigQuery costs as data grows.

### 4. CI/CD Pipeline (Medium Impact)

Add GitHub Actions to run `dbt build` on every pull request. Catches breaking changes before they hit production. Can include a slim CI run against a dev dataset.

### 5. Automated Export & Refresh (Medium Impact)

Schedule the Python export script to run after dbt build (via cron, Cloud Scheduler, or GitHub Actions). Automate the upload to Google Drive so the full pipeline is hands-free: `dbt build → export CSVs → upload to Drive → Tableau refreshes`.

### 6. Data Quality Monitoring (Medium Impact)

Add dbt source freshness checks, custom singular tests, and row count assertions. Consider dbt packages like dbt_expectations for advanced testing (distribution checks, anomaly detection).

### 7. Live Data Sources (Future)

Replace CSV seeds with live HRIS/ATS/Survey API connections using Fivetran, Airbyte, or custom Python ingestion scripts. This makes the pipeline truly automated — real data flowing in, not static CSVs.

### 8. Reporting Automation (Future)

Build automated insight generation on top of the mart tables — similar to the Exit Intelligence system at Attentive. LLM-powered summaries of attrition trends, engagement shifts, and compensation outliers delivered as scheduled reports.
