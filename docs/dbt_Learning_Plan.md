# dbt Learning Plan

**Tutorial:** [5hr dbt Masterclass — Ankit Lamba](https://www.youtube.com/watch?v=B8uwFmVt4sU)

**Method:** Watch each section, then apply the concept to the JustKaizen project. Push to both BigQuery (existing) and Snowflake (new).

**Your advantage:** You already have a working 25-model dbt project. Most learners are starting from zero. You're reverse-engineering what you built with Claude — this time understanding every layer.

---

## Phase 0: Snowflake Setup (Do This First)

**Goal:** Get a Snowflake trial running so you can deploy in parallel throughout the plan.

| Step | Action |
|------|--------|
| 1 | Sign up for [Snowflake free trial](https://signup.snowflake.com/) — 30 days, $400 in credits, no credit card |
| 2 | Create a warehouse (`JUSTKAIZEN_WH`), database (`JUSTKAIZEN_DEV`), and schema (`RAW`) |
| 3 | Install the Snowflake adapter: `pip3 install dbt-snowflake` |
| 4 | Add a `snowflake` target to your `~/.dbt/profiles.yml` (keep your existing `dev` BigQuery target) |
| 5 | Run `dbt debug --target snowflake` to confirm connection |

Your profiles.yml will look like this:

```yaml
justkaizen:
  target: dev                     # default = BigQuery
  outputs:
    dev:
      type: bigquery
      method: service-account
      project: just-kaizen-ai
      dataset: raw
      keyfile: ~/Documents/Claude_Code/Keys/just-kaizen-ai-6ee503c7c428.json
      threads: 4
    snowflake:
      type: snowflake
      account: <your-account-id>  # e.g., xy12345.us-east-1
      user: <your-username>
      password: <your-password>
      role: SYSADMIN
      warehouse: JUSTKAIZEN_WH
      database: JUSTKAIZEN_DEV
      schema: RAW
      threads: 4
```

**SQL dialect changes to watch for** (you'll fix these as you go):

| BigQuery | Snowflake | Where It Appears |
|----------|-----------|-----------------|
| `SAFE_DIVIDE(a, b)` | `DIV0(a, b)` or `IFF(b = 0, 0, a/b)` | fct_attrition_reporting, fct_recruiting_reporting, int_engagement_theme_rollup |
| `DATE_DIFF(end, start, MONTH)` | `DATEDIFF('month', start, end)` | int_employee_monthly_roster (tenure_months) |
| `LAST_DAY(date, MONTH)` | `LAST_DAY(date, 'month')` | int_employee_monthly_roster |
| `DATE_TRUNC(date, MONTH)` | `DATE_TRUNC('month', date)` | multiple models |
| `GENERATE_DATE_ARRAY(...)` | Use a recursive CTE or `GENERATOR` | dim_calendar |
| `COUNTIF(condition)` | `COUNT_IF(condition)` or `SUM(CASE WHEN ... THEN 1 ELSE 0 END)` | fct_attrition_reporting, int_engagement_theme_rollup |
| `COALESCE(bool_expr, false)` | Same syntax, but boolean handling differs | int_employee_monthly_roster |

---

## Phase 1: Foundations — "I Built This, Now I Understand It"

**Tutorial sections:** 0:00–1:08 (What is dbt, Why dbt, Platforms, Models intro)

You already know all of this conceptually. Watch at 1.5x for the vocabulary and framing — this is interview prep, not net-new learning.

### Exercise 1A: Narrate Your Own Architecture

Open your `dbt_project.yml` and `profiles.yml` side by side. For each config line, write a one-sentence comment explaining what it does and why you chose that value. You already have this in the System Guide — now make sure you can explain it without the guide open.

**Deliverable:** You can draw the JustKaizen architecture on a whiteboard from memory (seeds → staging → intermediate → marts → export → Tableau) and explain each layer's materialization choice.

### Exercise 1B: dbt debug + dbt compile

```bash
dbt debug                    # confirm BigQuery connection
dbt debug --target snowflake # confirm Snowflake connection
dbt compile                  # look at target/compiled/ — read the raw SQL for 2-3 models
```

Open `target/compiled/justkaizen_analytics/models/marts/fct_attrition_reporting.sql`. This is the exact SQL dbt sends to BigQuery. Read it. This is what the tutorial's `target/run/` folder explanation is about — you just have 25 models worth of compiled SQL to explore.

---

## Phase 2: Sources, Models & ref()

**Tutorial sections:** 1:08–1:51 (Databricks setup, first models, sources.yml, ref function, bronze layer)

Skip his Databricks setup entirely. You already have BigQuery + soon Snowflake.

### Exercise 2A: Trace the ref() Chain

Pick one mart model — `fct_attrition_reporting` is the most complex. Open it and trace every `{{ ref('...') }}` call. Write down the full dependency chain:

```
fct_attrition_reporting
  ← int_reporting_grid_attrition
      ← dim_calendar
      ← int_employee_monthly_roster
          ← stg_employees (← source: raw_employees)
          ← stg_recruiting (← stg_employees + source: raw_offers_hires)
          ← int_employee_compensation_current (← stg_employees + stg_comp_bands)
          ← int_employee_performance_history (← stg_performance)
          ← dim_calendar
  ← int_employee_monthly_roster (same as above)
```

**Deliverable:** A hand-drawn or Lucidchart DAG for fct_attrition_reporting showing every upstream dependency.

### Exercise 2B: Your sources.yml

Open `models/staging/sources.yml`. This is the tutorial's "sources" concept — you already have it. Read through it and make sure you understand: the `source()` function in staging models points here, and these point to the `raw_raw` dataset (BigQuery) or `RAW` schema (Snowflake).

### Exercise 2C: Deploy Seeds to Snowflake

This is where your hands hit the keyboard:

```bash
dbt seed --target snowflake
```

Watch for errors. The big one will be `raw_ees_responses.csv` (165K rows). Snowflake handles large seeds differently than BigQuery — note what happens and how long it takes.

---

## Phase 3: Configuration & Materialization

**Tutorial sections:** 1:51–2:22 (dbt_project.yml config, properties files, block-level config, custom schemas, node selection)

### Exercise 3A: Three Levels of Config in Your Project

The tutorial explains config priority: block > properties > dbt_project.yml. Find all three in your project:

1. **dbt_project.yml** — `+materialized: view` for staging, `+materialized: table` for marts
2. **Properties (schema.yml)** — your schema.yml files define tests per column
3. **Block-level** — `dim_calendar` has `+materialized: table` override in dbt_project.yml (this is between project-level and true block-level, but same concept)

**Exercise:** Add a true block-level config to one model. Open `models/intermediate/dim_calendar.sql` and add this at the top:

```sql
{{ config(materialized='table') }}
```

Run `dbt compile -s dim_calendar` and check `target/compiled/` — you'll see it resolves to a `CREATE TABLE` statement. This is redundant with your dbt_project.yml override, but proves you understand the precedence.

### Exercise 3B: Node Selection Practice

```bash
# Run just one model
dbt run -s stg_employees

# Run one model + everything downstream
dbt run -s stg_employees+

# Run everything upstream of a mart
dbt run -s +fct_attrition_reporting

# Run just the staging layer
dbt run -s models/staging/

# Run just one model against Snowflake
dbt run -s stg_employees --target snowflake
```

**Deliverable:** You're comfortable with `-s` (select) syntax and can target specific models or layers.

---

## Phase 4: Testing

**Tutorial sections:** 2:22–3:04 (Generic tests, singular tests, custom generic tests, severity, dbt test)

### Exercise 4A: Review Your Existing Tests

You have 124 schema tests across 3 files. Open each and categorize:

```bash
# Count tests per type
grep -c 'not_null' models/*/schema.yml
grep -c 'unique' models/*/schema.yml
grep -c 'accepted_values' models/*/schema.yml
```

The tutorial covers `unique`, `not_null`, `accepted_values`, and `relationships`. You use the first three. Consider where `relationships` tests would add value — e.g., does every `manager_id` in `stg_employees` actually exist as an `employee_id`?

### Exercise 4B: Write a Singular Test

The tutorial creates a "non-negative test" as a SQL file in `tests/`. Create one for your project. Example:

```sql
-- tests/assert_no_negative_tenure.sql
-- Tenure months should never be negative
select *
from {{ ref('int_employee_monthly_roster') }}
where tenure_months < 0
```

```sql
-- tests/assert_termination_date_after_hire.sql
-- Termination date should always be after hire date
select *
from {{ ref('int_employee_monthly_roster') }}
where termination_date is not null
  and termination_date < hire_date
```

Run them: `dbt test -s assert_no_negative_tenure assert_termination_date_after_hire`

### Exercise 4C: Write a Custom Generic Test

The tutorial builds a reusable `generic_non_negative` macro in `tests/generic/`. Build one for your project:

```sql
-- tests/generic/test_is_positive_or_zero.sql
{% test is_positive_or_zero(model, column_name) %}
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
```

Then apply it in `models/intermediate/schema.yml`:

```yaml
- name: tenure_months
  tests:
    - is_positive_or_zero
```

**Deliverable:** You have at least 2 singular tests and 1 custom generic test passing.

---

## Phase 5: Seeds (Deep Dive)

**Tutorial sections:** 3:04–3:14 (dbt seed, lookup tables, seed config)

You already use seeds heavily (6 CSVs, 165K+ rows total). But the tutorial treats seeds as small lookup/mapping tables, which is the more typical use case.

### Exercise 5A: Evaluate Your Seed Strategy

Your seeds are large HRIS/ATS exports — not typical lookup tables. In a production system, these would come from Fivetran/Airbyte, not CSV seeds. But for a portfolio project, seeds are fine.

**Interview talking point:** "I used dbt seeds for the source data in my portfolio project because it's self-contained and reproducible. In production, I'd connect Fivetran to the HRIS API and use dbt sources pointing at the raw tables Fivetran manages."

### Exercise 5B: Seed to Snowflake

If you haven't already:

```bash
dbt seed --target snowflake
```

Note the timing difference vs BigQuery. Snowflake's COPY INTO is fast for large CSVs — you may see faster load times than BigQuery for `raw_ees_responses`.

---

## Phase 6: Jinja & Macros

**Tutorial sections:** 3:14–4:16 (Jinja basics, writing macros, using macros in models, dbt packages)

This is the biggest learning gap between "I use dbt" and "I'm a dbt developer." The tutorial covers `{{ }}` (expressions), `{% %}` (statements), and macro creation.

### Exercise 6A: Find Jinja in Your Project

Search for existing Jinja usage:

```bash
grep -r '{{' models/ --include="*.sql" | head -20
```

You'll see `{{ ref('...') }}`, `{{ source('...', '...') }}`, and `{{ config(...) }}`. These are all Jinja. You've been using it — now understand the templating engine behind it.

### Exercise 6B: Write Your First Macro

Create a utility macro. A good one for your project:

```sql
-- macros/cents_to_dollars.sql
{% macro cents_to_dollars(column_name) %}
  round({{ column_name }} / 100.0, 2)
{% endmacro %}
```

Or more useful for your actual data — a macro that generates the excluded termination reasons list (currently hardcoded in `int_employee_monthly_roster`):

```sql
-- macros/excluded_termination_reasons.sql
{% macro excluded_termination_reasons() %}
  ('Reduction in Force', 'End of Contract', 'Entity Change',
   'Acquisition/Merger', 'End of Internship', 'International Transfer',
   'Relocation', 'Converting to FT')
{% endmacro %}
```

Then use it in `int_employee_monthly_roster.sql`:

```sql
-- Before:
em.termination_reason in (
    'Reduction in Force', 'End of Contract', ...
)
-- After:
em.termination_reason in {{ excluded_termination_reasons() }}
```

### Exercise 6C: Install dbt_utils

The tutorial covers dbt packages. Install one:

```yaml
# packages.yml (create in project root)
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
```

```bash
dbt deps
```

Explore what's available. `dbt_utils.generate_surrogate_key()`, `dbt_utils.star()`, and `dbt_utils.date_spine()` are the most commonly used. `date_spine` could replace your manual `GENERATE_DATE_ARRAY` in `dim_calendar` — and it's warehouse-agnostic, which solves your Snowflake compatibility issue.

**Deliverable:** At least one macro in `macros/`, `dbt_utils` installed, and you can explain what Jinja templating does in an interview.

---

## Phase 7: Snapshots (SCD Type 2)

**Tutorial sections:** 4:16–4:39 (Snapshots, slowly changing dimensions, snapshot strategies)

This is net-new for your project and a high-value interview topic.

### Exercise 7A: Understand the Problem Snapshots Solve

Your V1 design decision: "Static (latest-known) values for compensation, performance, and all employee attributes." This means if someone transferred from Engineering to Product in January, your historical data shows them in Product for every month — even months when they were in Engineering.

Snapshots fix this by tracking changes over time (SCD Type 2).

### Exercise 7B: Create a Snapshot

Snapshots use YAML config (dbt 1.8+). Create one for employees:

```yaml
# snapshots/snap_employees.yml
snapshots:
  - name: snap_employees
    relation: ref('stg_employees')
    config:
      schema: snapshots
      strategy: timestamp
      unique_key: employee_id
      updated_at: report_date
```

```bash
dbt snapshot
dbt snapshot --target snowflake
```

This creates a `snap_employees` table with `dbt_valid_from` and `dbt_valid_to` columns — the SCD Type 2 history you need for Phase 2 point-in-time joins.

**Deliverable:** A working snapshot model that you can explain as "this is how I'd track employee attribute changes over time in production."

---

## Phase 8: Incremental Models

**Tutorial:** Not covered in this masterclass, but referenced as a concept. Use the [dbt docs on incremental models](https://docs.getdbt.com/docs/build/incremental-models).

This is in your Evolution Recommendations (item #3). Build it.

### Exercise 8A: Convert a Mart to Incremental

`fct_attrition_reporting` is the largest table (274K rows). Convert it:

```sql
{{ config(
    materialized='incremental',
    unique_key=['report_month', 'department', 'sub_department', ...]
) }}

select ...
from ...

{% if is_incremental() %}
where report_month >= (select max(report_month) from {{ this }})
{% endif %}
```

Run it twice — first run does a full load, second run only processes new months.

**Deliverable:** At least one incremental model working in both BigQuery and Snowflake.

---

## Phase 9: CI/CD & Deployment

**Tutorial sections:** 4:39–5:06 (Git branching, multiple profiles/targets, dbt build for deployment, CI/CD workflows)

### Exercise 9A: Multi-Target Deployment

You already have two targets (BigQuery + Snowflake). The tutorial's "dev vs prod" concept maps directly:

```bash
dbt build --target dev          # BigQuery (development)
dbt build --target snowflake    # Snowflake (your "production" target)
```

### Exercise 9B: GitHub Actions CI

Create `.github/workflows/dbt_ci.yml`:

```yaml
name: dbt CI
on:
  pull_request:
    branches: [main]

jobs:
  dbt-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install dbt-bigquery
      - run: dbt deps
      - run: dbt build --target dev
        env:
          DBT_PROFILES_DIR: .
```

**Deliverable:** A CI pipeline that runs `dbt build` on every PR.

---

## Phase 10: Full Snowflake Deployment

**Goal:** Everything runs clean against Snowflake.

```bash
dbt seed --target snowflake
dbt build --target snowflake
```

Fix any remaining SQL dialect issues. Document what you changed in a `SNOWFLAKE_MIGRATION.md` — this is portfolio gold. It proves you're warehouse-agnostic.

---

## Interview Talking Points (What This Plan Gives You)

After completing this plan, you can speak to:

1. **"Walk me through your dbt project."** — Seeds → staging → intermediate → marts → export → Tableau. 25 models, 124 tests, 628K records.

2. **"How do you handle testing?"** — Generic tests (not_null, unique, accepted_values), singular tests (business logic validation), custom generic tests (reusable across models).

3. **"What's a macro?"** — Reusable Jinja template. I built one to centralize excluded termination reasons instead of hardcoding the same list in multiple models.

4. **"How would you handle slowly changing dimensions?"** — dbt snapshots with timestamp strategy. I built one for employee attributes to track department transfers and title changes over time.

5. **"How do you deploy?"** — Multi-target profiles.yml (BigQuery dev, Snowflake prod). GitHub Actions CI runs dbt build on every PR.

6. **"Is your project warehouse-specific?"** — No. I deployed the same models to both BigQuery and Snowflake. The main differences are date functions (DATE_DIFF vs DATEDIFF) and safe division (SAFE_DIVIDE vs DIV0). I documented all changes in a migration guide.

7. **"What would you do differently in V2?"** — Point-in-time joins using snapshots, incremental models for the large marts, dbt_utils.date_spine instead of manual date generation, automated export pipeline.
