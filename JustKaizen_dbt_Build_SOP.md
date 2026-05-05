# How the JustKaizen dbt Project Was Built: A Beginning-to-End Walkthrough

This document explains every step of the V1 build in the order it happened, what each piece does, and why it was built that way. Read this alongside the actual SQL files in your project.

---

## Step 0: Project Scaffolding (before any models)

### What was created:
- `dbt_project.yml` - The project config file
- `packages.yml` - External package dependencies
- `~/.dbt/profiles.yml` - Database connection (lives outside the repo)

### Why these exist:

**dbt_project.yml** tells dbt three things: what this project is called, where to find files, and how to materialize models.

```yaml
models:
  justkaizen_analytics:
    staging:
      +materialized: view      # Staging models are views (cheap, always fresh)
      +schema: staging         # Creates in raw_staging dataset
    intermediate:
      +materialized: view      # Intermediate models are views
      +schema: intermediate    # Creates in raw_intermediate dataset
    marts:
      +materialized: table     # Mart models are tables (fast for Tableau)
      +schema: marts           # Creates in raw_marts dataset
```

The materialization choice matters: views recompute every time someone queries them (cheap to create, slower to query). Tables are physical copies (costs storage, fast to query). Staging and intermediate are views because they're not queried directly. Marts are tables because Tableau reads them repeatedly.

**packages.yml** pulls in dbt-utils, which gives you helper functions like `generate_date_array` (used in dim_calendar).

**profiles.yml** stores your BigQuery connection details (project ID, service account key, dataset name). This file lives at `~/.dbt/profiles.yml` on your machine, NOT in the repo, because it contains credentials.

### How to verify:
```bash
dbt debug    # Tests the connection to BigQuery
dbt deps     # Installs packages from packages.yml
```

---

## Step 1: Sources Definition

### File: `models/staging/sources.yml`

### What it does:
Tells dbt where to find the raw seed tables in BigQuery.

```yaml
sources:
  - name: raw
    database: just-kaizen-ai
    schema: raw_raw
    tables:
      - name: raw_employees
      - name: raw_performance
      - name: raw_offers_hires
      - name: raw_ees_responses
      - name: raw_comp_bands
      - name: raw_job_history
```

### Why it exists:
When a staging model says `{{ source('raw', 'raw_employees') }}`, dbt looks up this file to find the actual BigQuery table location. If the dataset name changes (e.g., you move from `raw_raw` to `production_raw`), you change it here once and every model that references it updates automatically.

Without sources.yml, you'd hardcode table names like `just-kaizen-ai.raw_raw.raw_employees` in every staging model. That breaks the moment anything moves.

### dbt concept: `{{ source() }}` vs `{{ ref() }}`
- `{{ source() }}` points to raw tables that exist OUTSIDE dbt (your seed tables, or in production, tables loaded by Fivetran/Airbyte)
- `{{ ref() }}` points to other dbt models. This is how dbt builds the dependency graph.

---

## Step 2: Seed Data Loading

### What happened:
```bash
dbt seed    # Loads all CSVs from seeds/ into BigQuery
```

This created 6 tables in the `raw_raw` dataset:
- raw_employees (1,900 rows)
- raw_performance (9,024 rows)
- raw_offers_hires (5,658 rows)
- raw_ees_responses (165,984 rows)
- raw_comp_bands (523 rows)
- raw_job_history (2,681 rows)

### Why seeds and not direct uploads:
Seeds are version-controlled CSVs that dbt loads for you. The alternative is manually uploading CSVs to BigQuery (what you did at Attentive). Seeds are better because:
- They're in git, so they're versioned and reproducible
- `dbt seed` is one command, not 6 manual uploads
- Column types can be pinned in dbt_project.yml so dates don't load as strings

### Limitation:
Seeds are for small, static datasets (reference tables, test data, synthetic data). In production, raw data comes from Fivetran, Airbyte, or direct API loads, not seeds. Your sources.yml would point to those tables instead.

---

## Step 3: dim_calendar (The Date Spine)

### File: `models/intermediate/dim_calendar.sql`

### What it does:
Generates one row per day from January 1, 2020 through December 31, 2026.

### Why it was built first:
dim_calendar has zero dependencies. It doesn't read from any other model. Everything downstream that needs a date reference (the roster, the reporting grids) reads from dim_calendar. Building it first means everything else has a date spine to work with.

### Key pattern: GENERATE_DATE_ARRAY
```sql
SELECT
    calendar_date,
    DATE_TRUNC(calendar_date, MONTH) AS report_month,
    ...
FROM UNNEST(GENERATE_DATE_ARRAY('2020-01-01', '2026-12-31')) AS calendar_date
```

This is a BigQuery-specific function that generates an array of dates. UNNEST turns the array into rows. The result is 2,557 rows, one per day.

### Why not just use report_month everywhere:
You might think "I only need months, not days." But having days gives you:
- `is_month_end` flag (needed for end-of-month snapshots)
- `is_quarter_end` flag (needed for quarterly reporting)
- Flexibility for Phase 2 (daily employee snapshots)

### Materialization override:
dim_calendar is materialized as a TABLE even though it's in the intermediate layer (which defaults to view). This is set in dbt_project.yml because this model is referenced by nearly every other model. Making it a table means BigQuery doesn't regenerate 2,557 rows every time something references it.

---

## Step 4: Staging Models (6 views)

### Files:
- `models/staging/stg_employees.sql`
- `models/staging/stg_performance.sql`
- `models/staging/stg_recruiting.sql`
- `models/staging/stg_engagement.sql`
- `models/staging/stg_comp_bands.sql`
- `models/staging/stg_job_history.sql`

### What they do:
Each staging model reads from one raw source table and applies ONLY:
1. Field renaming (snake_case, clearer names)
2. Type casting (string dates to DATE, string numbers to FLOAT)
3. Source-level filtering (stg_performance filters to manager reviews only)

### What they do NOT do:
- No joins between tables
- No calculated fields (no compa_ratio, no tenure_months)
- No aggregation
- No business logic

### Why this rule exists:
Staging is the boundary between "vendor format" and "our format." If ADP changes a field name tomorrow, you fix stg_employees.sql and nothing else breaks. Every model downstream reads from staging, never from raw. This is the single most important dbt convention.

### The CTE pattern:
Every staging model follows the same structure:
```sql
WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_employees') }}   -- Read from raw
),

renamed AS (
    SELECT
        Work_Email AS employee_id,          -- Rename
        SAFE.PARSE_DATE(...) AS hire_date,  -- Cast
        ...
    FROM source
)

SELECT * FROM renamed
```

**WITH ... AS** creates a Common Table Expression (CTE). Think of it as a named temporary table that exists only for this query. The pattern is:
1. `source` CTE: grab the raw data
2. `renamed` CTE: clean it up
3. Final SELECT: output the cleaned version

### Notable staging decisions:

**stg_performance** is the only staging model with real filtering:
```sql
WHERE Response_Type = 'manager'
  AND Question = 'Performance Category'
```
The raw Lattice export has one row per reviewer per question per employee per cycle. This filter reduces it to one official rating per employee per cycle. This is a source-level filter (it defines what data we're pulling from this source), not business logic.

**stg_recruiting** renames `Source` to `application_channel` because `source` is a reserved word in BigQuery SQL. This would cause subtle bugs in GROUP BY and ORDER BY clauses downstream.

**stg_comp_bands** parses salary strings like "$174,000" into numeric 174000.0:
```sql
SAFE_CAST(REPLACE(REPLACE(Zone_A_Min_Salary, '$', ''), ',', '') AS FLOAT64)
```

---

## Step 5: Intermediate Helper Models (3 views)

### Files:
- `models/intermediate/int_employee_tenure.sql`
- `models/intermediate/int_employee_compensation_current.sql`
- `models/intermediate/int_employee_performance_history.sql`

### Why these exist:
The roster (Step 6) needs one row per employee with their latest salary, latest performance rating, and tenure metrics. But the source tables have MULTIPLE rows per employee (multiple comp changes, multiple review cycles, multiple job history events). These helper models collapse many-to-one.

### The ROW_NUMBER() pattern:
```sql
ROW_NUMBER() OVER (
    PARTITION BY employee_id 
    ORDER BY review_completed_date DESC
) = 1
```

This is the most important SQL pattern in analytics engineering. It says: "For each employee, number their rows from newest to oldest. Keep only row 1 (the newest)."

**int_employee_compensation_current** uses this to find the latest salary per employee, then joins to stg_comp_bands to get the matching band (Zone A or B based on Pay_Zone):
```sql
CASE
    WHEN e.employee_zone = 'ZONE A' THEN cb.zone_a_mid_salary
    WHEN e.employee_zone = 'ZONE B' THEN cb.zone_b_mid_salary
END AS comp_band_mid
```

**int_employee_performance_history** uses this to find the latest review rating per employee and derives top_performer_flag:
```sql
CASE
    WHEN overall_rating_numeric >= 4 THEN 'Y'
    ELSE 'N'
END AS top_performer_flag
```

(The critical_talent OR condition is added later in the roster, not here, because critical_talent lives on stg_employees, not stg_performance.)

---

## Step 6: The Employee Monthly Roster (the golden record)

### File: `models/intermediate/int_employee_monthly_roster.sql`

### What it does:
Creates one row per employee per month they were active (or terminated in that month). This is the most important model in the project. Everything downstream reads from it.

### Construction logic (5 CTEs):

**CTE 1: calendar_months**
```sql
SELECT DISTINCT report_month, report_quarter
FROM dim_calendar
WHERE is_month_end = TRUE
```
Gets every month in the date spine. One row per month.

**CTE 2: employees_latest**
```sql
SELECT *, ROW_NUMBER() OVER (
    PARTITION BY employee_id 
    ORDER BY report_date DESC
) AS rn
FROM stg_employees
WHERE rn = 1
```
Deduplicates to one row per employee (latest snapshot). Your raw_employees has one Report_Date (MVP), but this handles multiple snapshots for Phase 2.

**CTE 3: employee_months (THE CROSS JOIN)**
```sql
SELECT e.*, m.report_month, m.report_quarter
FROM employees_latest e
CROSS JOIN calendar_months m
WHERE e.hire_date <= LAST_DAY(m.report_month)
  AND (e.termination_date IS NULL 
       OR e.termination_date >= m.report_month)
```

This is the core of the architecture. CROSS JOIN means "every employee paired with every month." The WHERE clause then filters to only the months where that employee was active. An employee hired in June 2021 and terminated in November 2024 appears in months June 2021 through November 2024 and nowhere else.

At Attentive, you did this manually each month: download the census, add it to the combined table. The CROSS JOIN does it for every month at once.

**CTE 4: enriched**
LEFT JOINs the three helper models (comp, performance, tenure) and stg_recruiting onto the employee_months CTE. This is where the row gets enriched with salary, compa_ratio, latest rating, top_performer_flag, candidate_source, etc.

**CTE 5: final**
Computes all derived fields:
- `level_group` (CASE on job_level)
- `tenure_months` (DATE_DIFF from hire_date)
- `tenure_bucket` (CASE on tenure_months)
- `new_hire_flag` ("New Hire" if <= 12 months)
- `employment_status` ("Active" or "Terminated")
- `is_terminated_this_month` (boolean)
- `is_excluded_termination` (checks against the excluded reasons list)
- `is_attrition_eligible_term` (terminated this month AND not excluded)
- `flag_latest_report` (MAX report_month)

### Why all derived fields live here:
If the tenure bucket definition changes from 1-year to 6-month intervals, you change it in ONE place (this model) and every downstream mart automatically picks up the change. This is the semantic layer concept. The roster defines what every term means. Nothing downstream re-derives anything.

---

## Step 7: Reporting Grids (4 views)

### Files:
- `models/intermediate/int_reporting_grid_attrition.sql`
- `models/intermediate/int_reporting_grid_recruiting.sql`
- `models/intermediate/int_reporting_grid_workforce.sql`
- `models/intermediate/int_reporting_grid_compensation.sql`

### What they do:
Create a scaffold of every (month x dimension combination) that has ever existed. 

### Why they exist:
Without grids, a month where Engineering has zero voluntary terminations would be a MISSING ROW in the attrition mart. The trend line in Tableau would have a gap. The TTM rolling window would miscalculate because it's missing a month.

The grid ensures every cell exists, even with zero values. The mart LEFT JOINs actual data onto the grid, and COALESCE fills missing values with 0.

### The pattern:
```sql
WITH months AS (
    SELECT DISTINCT report_month FROM dim_calendar
),
dimension_combos AS (
    SELECT DISTINCT department, sub_department, job_level, ...
    FROM int_employee_monthly_roster
)
SELECT m.report_month, d.*
FROM months m
CROSS JOIN dimension_combos d
```

Another CROSS JOIN, but this time it's months x dimension combos, not months x employees. The result is the complete scaffold.

---

## Step 8: Reporting Marts (4 tables)

### Files:
- `models/marts/fct_attrition_reporting.sql`
- `models/marts/fct_recruiting_reporting.sql`
- `models/marts/fct_workforce_composition.sql`
- `models/marts/fct_compensation_reporting.sql`

### What they do:
Aggregate the roster by the grid dimensions, LEFT JOIN onto the grid, compute rolling windows and benchmarks. These are the tables Tableau reads.

### The construction pattern (using fct_attrition_reporting as the example):

**CTE 1: cell_aggregated**
Groups the roster by all grid dimensions and counts:
```sql
SELECT
    report_month,
    department,
    gender,
    ...,
    COUNT(DISTINCT CASE WHEN employment_status = 'Active' THEN employee_id END) AS end_month_headcount,
    COUNT(DISTINCT CASE WHEN is_attrition_eligible_term THEN employee_id END) AS total_terminations,
    COUNT(DISTINCT CASE WHEN is_attrition_eligible_term AND termination_type = 'Voluntary' THEN employee_id END) AS voluntary_terminations
FROM int_employee_monthly_roster
WHERE employment_type = 'Full Time'
GROUP BY report_month, department, gender, ...
```

COUNTIF with CASE WHEN is how you count different subsets in one pass. Each CASE WHEN defines a different filter for a different metric.

**CTE 2: scaffolded**
LEFT JOINs the aggregated data onto the reporting grid:
```sql
SELECT
    g.*,
    COALESCE(a.end_month_headcount, 0) AS end_month_headcount,
    COALESCE(a.total_terminations, 0) AS total_terminations,
    ...
FROM int_reporting_grid_attrition g
LEFT JOIN cell_aggregated a
    ON g.report_month = a.report_month
    AND g.department IS NOT DISTINCT FROM a.department
    AND g.gender IS NOT DISTINCT FROM a.gender
    AND ...
```

`IS NOT DISTINCT FROM` instead of `=` handles NULL values. If both sides are NULL, `=` returns NULL (not TRUE), which would drop the row. `IS NOT DISTINCT FROM` treats NULL = NULL as TRUE.

**CTE 3: with_ttm (the rolling window)**
```sql
SUM(total_terminations) OVER (
    PARTITION BY department, sub_department, ...
    ORDER BY report_month
    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
) AS ttm_total_terminations,

AVG(end_month_headcount) OVER (
    PARTITION BY department, sub_department, ...
    ORDER BY report_month
    ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
) AS ttm_avg_headcount
```

This is the TTM calculation. The window function says: "For each row, look at this row plus the 11 rows before it (= 12 months total). Sum the terminations. Average the headcount."

PARTITION BY means the window resets for each dimension combination. Engineering's window is separate from Sales's window.

**CTEs 4-5: orgwide_ttm and dept_ttm**
Same calculation but partitioned differently: orgwide has no PARTITION BY (whole company), dept has PARTITION BY department only. These get joined back as benchmark columns.

**CTE 6: final**
Joins everything together and computes rates:
```sql
SAFE_DIVIDE(ttm_total_terminations, ttm_avg_headcount) AS ttm_overall_attrition_rate
```

SAFE_DIVIDE returns NULL instead of erroring on division by zero.

---

## Step 9: Detail/Drill-through Marts (4 tables)

### Files:
- `models/marts/fct_employee_roster.sql` (promotes the roster to a table)
- `models/marts/fct_recruiting_velocity.sql` (promotes funnel metrics)
- `models/marts/fct_engagement_trends.sql` (promotes theme rollup)
- `models/marts/fct_performance_distribution.sql` (joins performance + employees)

### Why these exist separately from the reporting marts:
The reporting marts are aggregated (one row per month x dimension combo). When someone asks "who specifically left?", you need the individual employee record. That's the drill-through table.

fct_employee_roster is essentially: `SELECT *, a few display fields FROM int_employee_monthly_roster`. It's the same data, just materialized as a table (fast for Tableau) with a few convenience columns added:
- `band_position_label` ("Below Band" / "Within Band" / "Above Band")
- `is_active` (boolean)

---

## Step 10: Running It All

### The commands in order:
```bash
dbt deps                    # Install dbt-utils package
dbt seed                    # Load CSVs into BigQuery (raw_raw dataset)
dbt run                     # Build all 25 models in dependency order
```

dbt automatically figures out the build order from the `{{ ref() }}` calls. It knows dim_calendar has no dependencies, so it builds first. It knows fct_attrition_reporting depends on int_reporting_grid_attrition which depends on int_employee_monthly_roster which depends on stg_employees, so it builds them in that order.

You never tell dbt "build this before that." The ref() calls ARE the dependency graph.

### What happened during our build:
- dim_calendar: built as TABLE (2,557 rows)
- 6 staging views: created in seconds (views don't store data)
- 3 helper intermediate views: created
- 1 roster view: created (this is the heaviest view, ~78k rows when queried)
- 4 grid views: created
- 8 mart tables: created (the data materializes here, largest is fct_attrition_reporting at 492k rows)

### Errors we hit and what they taught:
1. **stg_job_history**: a comment containing `*/` terminated the block comment prematurely. Lesson: BigQuery parses `/* */` comments greedily. Don't put `*/` inside comments.
2. **stg_recruiting**: DATE_DIFF got STRING arguments. Lesson: CSV seeds load dates as strings unless you pin types. Always SAFE.PARSE_DATE in staging.
3. **int_employee_tenure**: COALESCE mixed DATE and STRING. Lesson: BigQuery is strict about type matching in COALESCE. Both arguments must be the same type.

These are the exact errors you'd hit in production. Knowing how to diagnose them (read the compiled SQL in target/compiled/) is a core dbt skill.

---

## Summary: Why Each Layer Exists

| Layer | Job | Real-world analogy |
|-------|-----|-------------------|
| Seeds | Load raw CSVs into the warehouse | The monthly CSV exports you downloaded at Attentive |
| Sources.yml | Tell dbt where to find the raw data | The bookmark to your OneDrive folder |
| Staging | Clean, rename, cast. One model per source. | Opening the CSV and fixing the column headers |
| Intermediate helpers | Collapse many-to-one (latest comp, latest rating) | The lookups you did before building the master table |
| dim_calendar | Generate a complete date spine | The PSA_Calendar_2023_2025 table at Attentive |
| Roster | One row per employee per month, all dimensions | Your Combined_Employee_Report at Attentive (EGD-05) |
| Reporting grids | Ensure no gaps in trend lines | Drawing the blank grid before filling in numbers |
| Reporting marts | Pre-compute rolling metrics and benchmarks | Your Tableau_Attrition_Rates table at Attentive (EGD-06B) |
| Detail marts | Individual records for drill-through | Your Latest_Employee_Report at Attentive (EGD-06E) |
