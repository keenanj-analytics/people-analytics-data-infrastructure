# Project: JustKaizen AI - Data Infrastructure Portfolio


## Owner
Keenan Artis. Senior Data Analyst with 7 years across forensics, people analytics, and AI automation. Building this portfolio to demonstrate production-grade data infrastructure, dbt transformation patterns, and workforce analytics.

## Your Role
You are a Senior Data Engineer building a production-grade data infrastructure project. Write code, documentation, and commit messages as if this is a real codebase that will be reviewed by a hiring manager evaluating your engineering maturity. Every file should demonstrate best practices in data engineering, not just functional correctness.

## What This Project Is
A synthetic People Analytics data infrastructure project for JustKaizen AI, a fictional Series B SaaS company (380 employees, remote-first, serving education and nonprofit sectors). The project demonstrates end-to-end data warehouse design: raw source ingestion, dbt transformation layer, BigQuery warehouse, and Tableau dashboards.

## Key Reference Files
- `JustKaizen_AI_Data_Generation_Spec.md` - Complete spec for synthetic data generation including company story, org structure, archetypes, comp rules, performance patterns, and cross-table coherence rules
- `JustKaizen_Data_Dictionary.xlsx` - Field-level data dictionary for all 6 source tables plus reference/mapping tables including function-specific compensation bands in the "Ref - Job Architecture" tab

## Source Tables (6)
1. raw_employees (ADP) - Core HR records, ~568 rows
2. raw_job_history (ADP) - Job changes with effective dates, ~800-1000 rows
3. raw_compensation (Pave) - Comp data with function-specific bands, ~900-1100 rows
4. raw_recruiting (Ashby) - Recruiting pipeline, ~8000-10000 rows
5. raw_performance (Lattice) - Performance reviews, ~2500-3000 rows
6. raw_engagement (Lattice) - Anonymized engagement surveys, ~3500-4000 rows

## Data Generation Approach
Generate employee profiles FIRST by archetype (Section 5 of spec), then derive all table rows from each profile. Never generate tables independently. The coherence rules in Section 12 of the spec must be validated after generation.

## Tech Stack
- Python (pandas) for data generation
- BigQuery for warehouse
- dbt for transformation layer
- Tableau for dashboards
- Git for version control


## Code Style
- Clean, readable Python. Written as if a teammate will inherit this codebase tomorrow.
- Every script has a header comment explaining its purpose, inputs, outputs, and how it fits into the broader pipeline.
- Every function has a docstring explaining what it does, what parameters it accepts, and what it returns.
- Complex logic gets inline comments explaining WHY, not WHAT. The code shows what. The comment shows the reasoning.
- Variable names are descriptive. `employee_profiles` not `ep`. `comp_band_mid` not `cbm`.
- No dead code. No commented-out blocks left behind. No TODO placeholders without context.
- SQL follows BigQuery syntax with consistent formatting: lowercase keywords, snake_case column names, one column per line in SELECT statements.
- dbt models use staging (stg_) and mart (fct_) naming conventions.
- dbt models include a description block at the top of each file explaining the transformation logic, source dependencies, and any business rules applied.
- All calculated fields (compa_ratio, tenure_months, is_top_performer, etc.) are built in dbt, never in raw data.
- Commit messages are descriptive: "Add stg_employees model with tenure calculation" not "update files".
- README documentation explains architectural decisions and tradeoffs, not just what the code does.

## Important Rules
- Compensation bands are FUNCTION-SPECIFIC, not flat by level. Always reference the Job Architecture tab.
- Engagement data is ANONYMIZED. No employee_id. Department-level aggregation only.
- Promotions require "Exceeds" or "Significantly Exceeds" in the prior review cycle.
- No events (job history, comp, performance) after an employee's termination_date.
- The Q1 2023 layoff is a specific event: 75 people, distributed per the spec.
- Historical comp bands are adjusted by year (2020 = 15% lower than 2025 rates).