# JustKaizen AI — Data Infrastructure

End-to-end synthetic People Analytics warehouse for **JustKaizen AI**, a fictional Series B SaaS company (380 active employees, remote-first, education / nonprofit verticals). Six raw source tables generated in Python, validated for cross-table coherence, then transformed through a dbt-on-BigQuery staging → intermediate → marts pipeline that's ready to plug into Tableau.

The portfolio goal is to demonstrate production-grade data infrastructure end to end: realistic synthetic data, archetype-driven generation, Section-by-Section spec adherence, dbt layer architecture, dimensional modeling.

---

## Project at a glance

| | |
|---|---|
| **Synthetic profiles** | 604 employees (380 active + 224 terminated) |
| **Raw rows generated** | 19,009 across 6 tables |
| **dbt models** | 19 (6 staging + 7 intermediate + 6 marts) |
| **dbt tests** | ~150 column tests + 5 compound-PK tests |
| **Section 12 coherence validation** | 20 hard rules, 0 violations on the shipped CSVs |
| **Snapshot date** | 2025-03-31 (the spec's current quarter) |

---

## Architecture

### Pipeline overview

```
[ Python generation ]                       [ dbt warehouse build ]
─────────────────────                       ──────────────────────

scripts/data_generation/                    seeds/                  bq raw.*
  01 generate_employee_profiles
  02 designate_manager_layer                  raw_employees.csv      ─┐
  03 resolve_manager_hierarchy                raw_job_history.csv     │
  04 audit_subdept_level_grid                 raw_compensation.csv    │  dbt seed
  05 align_subdept_level_grid                 raw_performance.csv     │  ─────────►
  06 build_demographics                       raw_recruiting.csv      │
  07 materialize_raw_employees                raw_engagement.csv     ─┘
  08 complete_raw_job_history
  09 build_raw_compensation                                        bq raw.*
  10 build_raw_performance                          │
  11 build_raw_recruiting                           ▼
  12 build_raw_engagement                  models/staging/  ─►  bq staging.*
  13 validate_and_export                            │             (views)
                                                    ▼
                                          models/intermediate/  ─►  bq intermediate.*
                                                    │                (views)
                                                    ▼
                                              models/marts/  ─►  bq marts.*
                                                    │             (tables)
                                                    ▼
                                                Tableau
```

### Layer strategy

| Layer | Materialization | Schema | Rationale |
|---|---|---|---|
| `seeds` | table | `raw` | One-time CSV load. Column types pinned via `+column_types` so dbt doesn't infer DATE-as-STRING or BOOL-as-INT64. |
| `staging/stg_*` | view | `staging` | Pure passthrough — type-cast and rename only. No business logic, no joins, no calculated fields. Cheap to refresh; small dataset means view performance is fine. |
| `intermediate/int_*` | view | `intermediate` | Joins, window functions, calculated fields (compa_ratio, is_top_performer, etc.). Started as ephemeral; switched to view because `int_employee_dimension` and `int_employee_tenure` feed many downstream consumers and inlining via ephemeral would cause repeated re-computation. |
| `marts/fct_*` | table | `marts` | Reporting-ready facts. Materialized as tables so BI queries hit a pre-computed table rather than recursively rebuilding the upstream graph on every dashboard load. |

### Models

**Staging** (6 — one per source table, 1:1 typed views):
- `stg_employees`, `stg_job_history`, `stg_compensation`, `stg_performance`, `stg_recruiting`, `stg_engagement`

**Intermediate** (7 — calculated fields and pre-joins):
- `int_employee_tenure` — per-employee tenure, promotion stats, career velocity
- `int_employee_compensation_current` — per-employee latest comp + compa_ratio + band_position
- `int_employee_performance_history` — per (employee, cycle) ratings + is_top_performer
- `int_employee_event_sequence` — chronological event stream with prev/next windows
- `int_recruiting_funnel_metrics` — per-requisition funnel volumes + time-to-fill / time-to-offer + conversion rates
- `int_engagement_theme_rollup` — 27 questions rolled up to 8 themes per (cycle, dept)
- `int_employee_dimension` — wide single-row-per-employee pre-join consumed by most marts

**Marts** (6 — fact tables for the BI workbook):
- `fct_workforce_overview` — quarterly headcount + flow time series
- `fct_compensation_parity` — per-employee comp with peer-cohort percentiles
- `fct_performance_distribution` — per (employee, cycle) ratings + dim context
- `fct_recruiting_velocity` — per-requisition funnel + time-to + dept peer averages
- `fct_engagement_trends` — theme rollups with company-comparison + post-layoff flag
- `fct_attrition_drivers` — per-terminated-employee attribution columns

---

## Running the pipeline

### Prerequisites

- Python 3.9+ with `pandas`, `openpyxl`
- BigQuery project + dataset
- `dbt-bigquery` (1.7+)
- A `~/.dbt/profiles.yml` entry named `justkaizen` pointing at your BigQuery dataset (sample format in the header comment of `dbt_project.yml`)

### Step 1 — Generate the synthetic source data (Python)

The Python pipeline is idempotent and seeded. Running `13_validate_and_export.py` runs the full 12-script chain in dependency order, validates every Section 12 rule, and writes the six raw CSVs to `seeds/`.

```bash
python3 scripts/data_generation/13_validate_and_export.py
```

The script prints a per-rule scorecard. It will refuse to write CSVs if any **hard** Section 12 rule fails; soft (informational) rule failures are reported but not blocking. The shipped CSVs pass all 20 hard rules.

### Step 2 — Load + transform (dbt)

```bash
dbt deps                            # install dbt-utils (one-time)
dbt seed                            # load the 6 CSVs into BigQuery raw.*
dbt run                             # build all 19 models in dependency order
dbt test                            # run all ~150 column tests + compound PK tests
```

Selective rebuilds:
```bash
dbt run --select staging            # rebuild just the 6 stg_* views
dbt run --select int_employee_tenure+   # rebuild tenure and everything downstream of it
dbt run --select fct_workforce_overview --vars '{current_date: "2025-06-30"}'
                                    # override snapshot date for the run
```

---

## Architectural decisions

### Section 12 coherence validated upstream in Python, not in dbt

Section 12 has 20+ cross-table invariants (every employee has a Hire row with `effective_date = hire_date`; every Promotion has a matching Pave row on the same date; no events after termination_date; etc.). I implemented these as a Python validator (`13_validate_and_export.py`) that runs *before* CSVs are written to disk. The dbt-side staging tests then trust the loaded shape.

**Why this layering:** if the Python generator emits a coherence violation, no CSVs are written and the pipeline fails fast. Catching the bug in dbt would mean BQ has already paid the seed cost and downstream models silently break with no clear error trail. Putting the gate in Python makes the contract between the generator and the warehouse explicit.

**Tradeoff:** dbt's `relationships` tests still catch FK violations between the seeded tables (e.g., `stg_job_history.employee_id` → `stg_employees.employee_id`), but the more semantic invariants (e.g., "every Promotion event has a matching comp record on the same date") aren't re-tested in dbt. If a manual edit to a seed CSV broke a Section 12 rule, dbt wouldn't necessarily catch it. Acceptable for a portfolio dataset; in production this would be a `dbt test` *and* a Python pre-flight check.

### Calculated fields live in dbt, never in the raw data

Per CLAUDE.md and the data dictionary's per-table notes, fields like `compa_ratio`, `tenure_months`, `is_top_performer`, `time_to_fill_days`, `band_position`, `career_velocity_per_year` are *all* computed in dbt — staging passes the raw source rows through unchanged.

**Why:** the spec's calculated formulas might evolve (e.g., the People Ops team might switch `compa_ratio` to use `comp_band_max` as the denominator instead of `comp_band_mid`). Centralizing these in `models/intermediate/` means one file edit propagates everywhere. The raw_* CSVs stay aligned with the vendor schemas (ADP, Pave, Lattice, Ashby) which keeps the dataset re-usable as if it were ingested from real APIs.

### Snapshot date as a project variable, not `CURRENT_DATE()`

`var('current_date')` defaults to `'2025-03-31'` (the spec's current quarter). Every time-based intermediate / mart calc anchors on this var. Override at runtime: `dbt run --vars '{current_date: "2025-06-30"}'`.

**Why:** `CURRENT_DATE()` would silently re-compute tenure_months on every dbt run, drifting the dataset's analytics as wall-clock time advances. For a synthetic dataset frozen at 2025-Q1, that's wrong. For a real production warehouse, replacing the var with `CURRENT_DATE()` is a one-line change.

**Tradeoff:** anyone running the pipeline needs to know to override the var if they want a fresher snapshot. Documented in the dbt_project.yml header comment so the override syntax is discoverable.

### `int_employee_dimension` as the wide pre-join

Five marts need some combination of: employee identity, current org placement, demographics, tenure, current comp, and latest review outcome. Rather than each mart re-joining 4-5 upstream models, `int_employee_dimension` pre-joins them once. The marts then `ref()` this single wide model.

**Tradeoff:** wide views are sometimes a code smell (Kimball would prefer narrower dimensions per fact). Justified here because:
1. The dataset is small (604 rows) so the wide view materializes cheaply.
2. The five marts that consume it span very different slices (workforce, comp, performance, attrition, parity), so a narrower dim per fact would mean five tightly-similar dims duplicated.
3. dbt's `ref` lineage stays cleanest with one canonical dim.

If a future mart only needed identity + dept + level (e.g., a slim `dim_employee` for an OLAP cube), a separate skinny model would be added — `int_employee_dimension` doesn't preclude that.

### Manager Step-Back encoded as `Title Change`

The Manager Step-Back archetype's M1 → IC4 demotion needs a `change_type` value, but the data dictionary's domain (`Hire`, `Promotion`, `Lateral Transfer`, `Department Transfer`, `Title Change`, `Manager Change`) has no `Step-back` or `Demotion` value. I used `Title Change` since the title genuinely changes and it's the closest fit semantically.

**Tradeoff:** an analyst querying `WHERE change_type = 'Title Change'` will find these step-backs alongside any future "title-only" reclassifications. Future-proof solution would be to add a `Step-back` to the spec domain — not done here because the spec is the source of truth.

### Founder hire window — Q1 2020 + 2021 H1 (vs spec's "all Q1 2020")

Section 5 says all 30 founder/early-employee profiles were hired Q1 2020. Section 4's hire-volume table only allocates 12 hires to Q1 2020 (3 of which are the Section 2 leadership trio). That leaves only 9 Q1 2020 slots for the founder archetype.

Resolution: 9 founders hired Q1 2020, 18 hired in 2021 H1 (the earliest available "early employee" window). 27 total active founders + 3 already-counted leadership = 30, matching Section 5.

Documented at length in `01_generate_employee_profiles.py` and `02_designate_manager_layer.py`. This is the cleanest reading of two Section-level constraints that don't algebraically reconcile.

### dbt-utils for compound + range tests, not custom singular tests

`packages.yml` adds dbt-utils for:
- `unique_combination_of_columns` — compound PK tests on (employee_id, review_cycle), (employee_id, event_sequence), (survey_cycle, department, theme)
- `expression_is_true` — range tests like `compa_ratio between 0 and 1.5`, `tenure_months >= 0`
- `relationships` with `where:` — FK tests with predicates (e.g., `manager_id` is FK except for the CEO null)

**Tradeoff:** dbt-utils adds an external dependency. Acceptable because (a) it's the de-facto-standard dbt package, (b) writing 20+ singular tests in `tests/` would be more code with weaker reusability.

---

## Spec deviations (documented)

The spec is internally inconsistent in places. Where I had to deviate, the deviation is documented at the source. Summary:

### Headcount reconciliation (Stage 1)

Section 5's archetype percentages don't algebraically reconcile with `568 total / 380 active / 188 terminated`. With the rigid 75-person Q1 2023 layoff plus the four 100%-terminated archetypes at their stated proportions, the stated rates produce only ~291 active vs target 380.

**Resolution:** Steady Contributor inflated 25% → 45% (256 → after the user-reviewed adjustment 214); Steady and Internal Mover at 100% active (lost the 15% / 10% term per spec); the four 100%-terminated archetypes resized to the user-approved counts (Early Churner 35, Top Performer Flight Risk 25, Performance Managed Out 15, Manager Change Casualty 20).

Documented in `01_generate_employee_profiles.py` docstring.

### Supplemental terminated profiles (Stage 1.5)

To restore the spec's voluntary turnover semantics for Steady Contributor (15% term) and Internal Mover (10% term), 36 supplemental terminated profiles were appended (32 Steady + 4 Internal Mover, all hired 2021-2022, all Voluntary). Total goes from 568 → 604 / 380 / 224.

These 36 are the only profiles with a `manager_id` populated at Stage 1; the other 568 had `manager_id` populated in Stage 2b's hierarchy resolution.

### Section 3 sub-dept × level grid — 13 residual delta cells

The 2c step-2 alignment closes the IC1+39 / IC4-31 starting-level skew via archetype-budget-respecting promotions and sub-dept rebalancing. 13 cells remain off the Section 3 + uplift target after the alignment runs:

- **Founder IC track at IC5 outside Engineering** — Section 3 only has IC5 cells in Engineering. CS / Product founder IC5 designations spill into non-Section-3 cells (e.g., CS Implementation IC5, Product Design IC5).
- **People L&D M1 vacancy** — Section 3 row sums imply 4 People M1; Section 2 says 3. 2a designated 3 per Section 2, leaving Section 3's L&D M1 unfilled.
- **Sales SDR IC1 shortage (-5)** — flex pool doesn't have 10 IC1-starting Sales profiles after the no-demotion guard. Spillover lands in adjacent SDR / AE / SE cells.

Documented in `05_align_subdept_level_grid.py`.

### Performance Managed Out — 3 SR1 soft violations

Spec says Performance Managed Out profiles should show declining ratings in their last 2-3 cycles. Section 7's "late" distribution still has a 15% Meets weight, so the random walk can occasionally produce a non-declining last cycle. 3 of 15 PMO profiles hit this. Documented in the validator output as a soft (informational) rule.

To strictly enforce decline, the late distribution would need to drop the Meets weight to 0 — not done here because it would deviate from Section 7's stated probabilities.

### Row count vs spec targets

| Table | Spec target | Actual | Note |
|---|---|---|---|
| `raw_employees` | ~568 | 604 | Spec uses ~ (approximate). Reconciliation math forced 604. |
| `raw_job_history` | 800-1,000 | 1,045 | Slightly over due to organic Manager Change events bridging structural at-hire vs current manager differences. |
| `raw_compensation` | 900-1,100 | 2,312 | Significantly over because the spec's row math appears to omit Annual Review records. With the per-Jan-15 review rule the count roughly doubles. Documented in `09_build_raw_compensation.py`. |
| `raw_performance` | 2,500-3,000 | 2,676 | Within range. |
| `raw_recruiting` | 8,000-10,000 | 9,348 | Within range. Funnel rates are inflated above Section 9 targets because the Applied-stage rejection pool was capped to keep total volume in the spec range. |
| `raw_engagement` | 3,500-4,000 | 3,024 | Slightly under. Sub-department rows (Section 12: "only when 5+ respondents") were not generated; including them would land in spec range. |

---

## File structure

```
JustKaizen-Data-Infrastructure/
├── README.md                                        ← this file
├── CLAUDE.md                                        ← project guidance for Claude
├── JustKaizen_AI_Data_Generation_Spec.md            ← the canonical spec
├── JustKaizen_Data_Dictionary.xlsx                  ← schemas + Job Architecture bands
│
├── scripts/data_generation/                         ← Python pipeline (Stages 1-4)
│   ├── 01_generate_employee_profiles.py
│   ├── 02_designate_manager_layer.py
│   ├── 03_resolve_manager_hierarchy.py
│   ├── 04_audit_subdept_level_grid.py
│   ├── 05_align_subdept_level_grid.py
│   ├── 06_build_demographics.py
│   ├── 07_materialize_raw_employees.py
│   ├── 08_complete_raw_job_history.py
│   ├── 09_build_raw_compensation.py
│   ├── 10_build_raw_performance.py
│   ├── 11_build_raw_recruiting.py
│   ├── 12_build_raw_engagement.py
│   └── 13_validate_and_export.py                    ← runs the chain + Section 12 + writes CSVs
│
├── seeds/                                           ← raw CSVs (loaded via `dbt seed`)
│   ├── raw_employees.csv         604 rows
│   ├── raw_job_history.csv     1,045 rows
│   ├── raw_compensation.csv    2,312 rows
│   ├── raw_performance.csv     2,676 rows
│   ├── raw_recruiting.csv      9,348 rows
│   └── raw_engagement.csv      3,024 rows
│
├── dbt_project.yml                                  ← profile, vars, layer materializations
├── packages.yml                                     ← dbt-utils
│
├── models/
│   ├── staging/                                     ← 6 stg_* + schema.yml
│   ├── intermediate/                                ← 7 int_* + schema.yml
│   └── marts/                                       ← 6 fct_* + schema.yml
│
├── analyses/                                        ← sample mart-consumption queries
│   ├── headcount_growth_with_layoff_overlay.sql
│   ├── compa_ratio_gender_gap_by_cohort.sql
│   ├── manager_change_attrition_signal.sql
│   └── post_layoff_engagement_trough.sql
│
└── (macros/, tests/, snapshots/  — empty placeholders for the dbt project structure)
```

---

## Sample analyses

The `analyses/` folder contains four BigQuery-flavored SQL queries demonstrating mart-consumption patterns:

1. **`headcount_growth_with_layoff_overlay.sql`** — Quarterly net workforce change with the Q1 2023 layoff visible as a single bar against five years of growth. Hits `fct_workforce_overview`.
2. **`compa_ratio_gender_gap_by_cohort.sql`** — Gender pay parity *within* (department, job_level) cohorts where the cohort is large enough to be meaningful (n ≥ 10 per gender). Hits `fct_compensation_parity`.
3. **`manager_change_attrition_signal.sql`** — % of voluntary terminations that occurred within 6 months of a manager change. Surfaces the Manager Change Casualty archetype's defining pattern. Hits `fct_attrition_drivers`.
4. **`post_layoff_engagement_trough.sql`** — Theme-by-theme score change from the 2022-Q4 pre-layoff cycle to the 2023-Q2 trauma cycle, then to the 2025-Q1 recovery cycle. Quantifies the Section 10 narrative. Hits `fct_engagement_trends`.

---

## Acknowledgements

- Spec authored against the `JustKaizen_AI_Data_Generation_Spec.md` and `JustKaizen_Data_Dictionary.xlsx` reference files.
- Compensation bands sourced from the data dictionary's `Ref - Job Architecture` tab.
- Built end-to-end through iterative review with checkpoints at every stage so deviations from the spec were captured and ratified rather than silently accepted.
