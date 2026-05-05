# Handoff: JustKaizen AI Portfolio - Session 2

Use this as context for Claude Cowork or a new Claude thread. Attach the project folder so Claude can read all files.

---

## Who Keenan Is

Senior Data Analyst, 7+ years. PwC (forensics analytics, 3 years) then Attentive (people analytics, 4 years). Built Attentive's People Analytics infrastructure from scratch as the first and only analytics IC. Core stack: SQL, BigQuery, Tableau, Apps Script, GPT API. Python developing. dbt learned through this project.

Mutual separation from Attentive effective May 8, 2026. Currently applying to Grafana Labs (People Analytics Analyst) and Kruse Consulting (Senior Analytics Engineer, part-time). Accountability deadline: presenting portfolio to a peer on May 7, 2026.

Writing preferences: no em dashes, no buzzwords, no AI-sounding language, no hedging. Direct, specific, numbers over adjectives.

## How to Work With Keenan (Communication Patterns)

This section is critical. These patterns were developed over a full-day session and are the difference between productive collaboration and spinning wheels.

**He has ADHD. Adjust accordingly.**
- When he's overwhelmed, STOP adding information. Strip everything down to one action. "Open File 1. That's it." works. "Here are 31 things to do" does not.
- He loses track of files across a long session. When he asks where something is, just resurface it. Don't explain the filing system.
- Numbered sequences work, but keep them short (3-5 items max per response). If the full list is 15 items, show the next 3 and say "we'll get to the rest after these."

**He learns by building, not by reading.**
- Never say "go read the docs." Give him something to build and point him to the docs as reference when he has a specific question.
- The sample workbook worked because he could SEE the data before understanding how dbt creates it. Always show the output before explaining the process.
- When explaining architecture, trace a specific example through the system. "Follow EMP-101 from raw_employees through the roster to fct_attrition_reporting" is 10x more effective than "the staging layer feeds the intermediate layer."

**He needs the "why" before the "how."**
- He won't accept a design decision without understanding the reasoning. "Use static comp values" doesn't land. "Use static comp values because point-in-time comp would require a separate history table we don't have, and that's Phase 2" does.
- Connect every decision to something he already knows. "This is the same pattern as your EGD-05 script at Attentive, just split into separate dbt models" clicks instantly.

**Be direct and confident. No hedging.**
- "Do this. Here's why." works. "You might want to consider..." does not.
- When he's wrong, say so clearly with the correction. "That's not right. The join is on employee_id, not work_email, because..." He course-corrects well when given direct feedback.
- When he asks a binary question, give a binary answer first, then explain. "Yes, fix it now. Here's why." not a paragraph leading up to the answer.

**He gets imposter syndrome around AI tools.**
- He occasionally feels like a fraud for using Claude Code. Remind him that the architecture decisions, the business logic definitions, the ERDs, the data dictionary, and the Question Cascade framework are HIS work. Claude Code typed the SQL. He designed the system.
- Frame AI as a workflow tool, not a replacement. "You're the architect, Claude Code is the builder" is the framing that resonated.

**Recognize good work with specifics, not flattery.**
- "Your ERD is correct" is useful. "Great job!" is not.
- When something he built is genuinely impressive, say why with technical specifics. "The TTM rolling window with averaged denominator is the methodologically correct approach. Most analysts use a two-point average that breaks during RIF events." That landed.
- Don't over-praise. He can tell the difference between recognition and encouragement.

**Keep the energy practical, not motivational.**
- He responds to "here's the next concrete step" not "you've got this!"
- When wrapping a session, give him exactly what to do between sessions, not an inspirational sendoff.
- He prefers "go trace EMP-312 through the workbook" over "go study the architecture."

**He thinks out loud and iterates in real time.**
- He'll upload an ERD, ask for feedback, fix it in 5 minutes, and upload again. Match that pace. Give concise, specific feedback each round.
- He'll change his mind mid-conversation (e.g., switching tenure from 6-month to 1-year buckets, eliminating M5-M6 levels). That's not indecisiveness, it's refinement. Track the latest decision and update all docs to match.
- When he says "let me know what you think," he wants 2-3 specific observations, not a comprehensive review.

**Technical calibration:**
- His SQL is strong (7 years of production SQL). Don't over-explain SQL concepts.
- His dbt knowledge is new (learned through this project). DO explain dbt-specific concepts (materializations, refs, CTEs, the DAG).
- His Python is developing. Don't assume he can debug Python scripts, but he can read them and understand the logic.
- His Git is basic. Give exact commands, one at a time, with no assumed knowledge of branching or rebasing.

**Session management:**
- At the end of a session, always produce an updated handoff document capturing every decision made.
- When starting a new session, don't re-explain the project. He knows it. Start with "where did we leave off?" and reference the handoff.
- If the conversation gets long and context might degrade, proactively flag it and suggest saving state.

## What This Project Is

Production-grade People Analytics data warehouse for JustKaizen AI, a fictional pre-IPO enterprise AI company (1,200 active employees, ~1,900 total, remote-first). Demonstrates end-to-end data infrastructure modeled after a real production system Keenan built at Attentive.

**GitHub repos:**
- Infrastructure: https://github.com/keenanj-analytics/people-analytics-data-infrastructure
- Analytics: https://github.com/keenanj-analytics/justkaizen-workforce-analytics

---

## What's DONE (Session 1)

### Architecture & Documentation
- Company Profile (1,200 employees, pre-IPO, full org structure, comp bands, attrition targets)
- Data Dictionary (every field in every model)
- Model Dependency Map (every connection between models with join keys)
- Architecture Spec v2 (full model specifications)
- Raw layer ERD (5 source tables with PKs, FKs, relationships)
- Pipeline flow diagram (staging → intermediate → mart with all connections)
- Infrastructure repo README (portfolio-facing, links to all docs)
- Analytics repo README (placeholder for dashboard screenshots)
- Dashboard Design Blueprint with Question Cascade framework for all 6 views

### dbt Models (25 deployed to BigQuery, all passing)
**Staging (6 views):** stg_employees, stg_performance, stg_recruiting, stg_engagement, stg_comp_bands, stg_job_history

**Intermediate (11 views + 1 table):**
- dim_calendar (table, 2,557 rows)
- int_employee_tenure, int_employee_compensation_current, int_employee_performance_history (helper models)
- int_employee_monthly_roster (the golden record, one row per employee per month)
- int_reporting_grid_attrition, int_reporting_grid_recruiting, int_reporting_grid_workforce, int_reporting_grid_compensation (scaffolds)
- int_recruiting_funnel_metrics, int_engagement_theme_rollup (separate paths)

**Marts (8 tables):**
- fct_attrition_reporting (492k rows in BQ, 756 in Sheets via department-level summary)
- fct_recruiting_reporting (159k rows in BQ, 756 in Sheets)
- fct_workforce_composition (120k rows in BQ, 11k in Sheets via dept+gender+race)
- fct_compensation_reporting (106k rows in BQ, 19k in Sheets via dept+level+gender)
- fct_employee_roster (78k rows in BQ, 1,200 in Sheets via latest snapshot)
- fct_recruiting_velocity (1,900 rows)
- fct_engagement_trends (720 rows)
- fct_performance_distribution (9,024 rows)

### Seed Data
- 6 CSV files generated via Python, ~185k total rows
- 1,200 active + 700 terminated employees
- All coherence validation checks passing
- Deterministic (RNG seed 42)

### Data Delivery
- Apps Script V1 deployed: BigQuery → Google Sheets (8 tabs, one per mart)
- Large reporting marts are queried with department-level aggregation to fit Sheets limits
- fct_employee_roster filtered to latest snapshot only

### V0 Cleanup
- Old V0 models removed from BigQuery (int_employee_dimension, int_employee_event_sequence, int_monthly_headcount_snapshot, fct_monthly_metrics, fct_workforce_overview, fct_attrition_drivers, fct_compensation_parity)
- CLAUDE.md removed from public GitHub (kept locally for Claude Code)

---

## Key Design Decisions

| Decision | Choice |
|---------|--------|
| Company | 1,200 active, pre-IPO, enterprise AI, founded 2018 |
| Level framework | P1-P6 (IC), M1-M4 (Manager/Director), E1-E6 (Senior Leadership). No M5-M6. |
| Level groups | Junior IC (P1-P3), Senior IC (P4-P6), Manager (M1-M2), Director (M3-M4), Senior Leadership (E1-E6) |
| Tenure buckets | 1-year intervals: 0-1, 1-2, 2-3, 3-4, 4-5, 5+ Years |
| Gender values | ADP convention: "Men", "Women", "Non-Binary", "Not Specified" |
| Comp zones | Zone A (SF/NYC metro), Zone B (everywhere else) |
| Comp band match | By job_title. Zone selected by employee's Pay_Zone. |
| Compa-ratio | ROUND(salary / comp_band_mid, 2) |
| TTM attrition | SUM(terms over 12mo) / AVG(12 monthly end headcounts). Smooths RIF distortion. |
| Top performer | Rating >= 4 OR is_critical_talent = TRUE |
| Excluded terminations | RIF, End of Contract, Entity Change, Acquisition/Merger, End of Internship, International Transfer, Relocation, Converting to FT. Data cleaning convention. |
| Termination_Voluntary | BOOL. TRUE=voluntary, FALSE=involuntary. RIF identified by termination_reason for exclusion. |
| Regrettable termination | Separate from top_performer. People Ops designation. |
| Performance scale | Source 1-4 (1=best), target 1-5 (5=best). "Partially Meets" (2) is JustKaizen addition. |
| Engagement | Anonymized. MVP aggregates to department. Sub-dept/level/tenure possible in Phase 2. |
| Recruiting source of truth | ADP Hire_Recruiter for employee records, ATS Recruiter for pipeline reporting |
| Employment filter | Full Time only for all metrics |
| Roster snapshots | End-of-month only. Beginning-of-month and daily snapshots are Phase 2. |
| Roster comp | Static (latest-known). Point-in-time comp history is Phase 2. |
| Manager status | ADP-provided value. Recomputing from direct report count is Phase 2. |
| AI-assisted development | Claude Code used for code development, model generation, testing. Credited in README. |
| Source system names | Generic (HRIS, ATS, Performance Management) in README. Not ADP/Lattice/Ashby. |

---

## What Needs to Be Done (Session 2)

### Priority 1: Tableau Dashboards (6 views)
Data is in Google Sheets. Tableau Public connects via Google Drive.

| View | Primary Data Source (Sheets tab) | What It Shows |
|------|--------------------------------|---------------|
| Workforce Overview | fct_workforce_composition + fct_employee_roster | Headcount trend, hires vs terms, department breakdown, lifecycle KPIs |
| Attrition | fct_attrition_reporting + fct_employee_roster | TTM attrition rates (overall, voluntary, top performer, regrettable), department comparison, tenure band, termination reasons |
| Hiring | fct_recruiting_reporting + fct_recruiting_velocity | Hiring volume, time to fill, offer acceptance, source mix |
| Compensation | fct_compensation_reporting + fct_employee_roster | Compa-ratio distribution, dept x level heatmap, gender pay gap |
| Engagement | fct_engagement_trends | eNPS trend, theme heatmap, theme vs company avg |
| Performance | fct_performance_distribution | Rating distribution, dept comparison, top performer concentration |

Dashboard Design Blueprint is in Google Drive with Question Cascades, data source mappings, and chart inventories for all 6 views.

### Priority 2: Kruse Consulting Application
- Resume tailoring to JD (dbt, BigQuery, Python, Tableau, AI/ML, part-time/entrepreneurial)
- Cover letter / outreach message
- Portfolio link ready: https://github.com/keenanj-analytics/people-analytics-data-infrastructure

### Priority 3: GitHub Finalization
- Update README: remove specific source system names (ADP → HRIS, Lattice → Performance Management, Ashby → ATS)
- Add dashboard screenshots to analytics repo
- Write Key Findings and Recommendations after dashboard analysis
- Pin both repos on GitHub profile

### Priority 4: Schema Tests
- schema.yml for all models (unique, not_null, accepted_values, relationships)
- Currently no tests written. Models deploy clean but aren't formally tested.

### Priority 5: Polish
- LinkedIn article + posts about the project
- YouTube walkthrough video (stretch)
- Present to accountability partner May 7

---

## Attentive Architecture Reference

The V1 JustKaizen architecture mirrors what Keenan built at Attentive. Key patterns replicated:

1. Master employee roster (one row per employee per month, all dimensions as columns)
2. Domain-specific reporting marts with TTM rolling windows
3. Org-wide and department benchmarks on every mart row
4. Full time grid (calendar + reporting grids) for gap-free trend lines
5. Excluded termination reasons as a data cleaning convention
6. Pre-hire fields joined via employee linkage (recruiter, source, hiring manager)
7. Comp band matching by job title with zone-based band selection

Source SQL files from Attentive are available in the conversation history but NOT in any public repo. They're reference material only.

---

## BigQuery Setup

- GCP project: just-kaizen-ai
- Service account key: /Users/keenanj/Documents/Claude_Code/Keys/just-kaizen-ai-6ee503c7c428.json
- profiles.yml: ~/.dbt/profiles.yml (profile: justkaizen, dataset: raw)
- Datasets: raw_raw (seeds), raw_staging (views), raw_intermediate (views), raw_marts (tables)
- dbt requires: `export PATH="$HOME/Library/Python/3.9/bin:$PATH"`

## Terminal Commands

```bash
dbt deps && dbt seed && dbt run && dbt test   # Full rebuild
dbt run --select model_name                     # Single model
dbt run --select model_name+                    # Model + downstream
git add . && git commit -m "message" && git push
```

## Google Sheets Data Warehouse

- Sheet: "JustKaizen Data Warehouse" (or new sheet with V1 Apps Script)
- 8 tabs, one per mart table
- Reporting marts use department-level summary queries (not full mart dumps)
- Refresh: JustKaizen Data menu > Refresh All Data

## Interview Prep Context

Keenan can explain:
- Why the roster is the center of the architecture (single source of truth, all dimensions as columns)
- Why TTM attrition uses AVG of 12 monthly headcounts as denominator (smooths RIF distortion)
- Why excluded terminations are a data cleaning convention, not a dashboard filter
- Why domain-specific marts instead of one mega-table (different dimensions per domain, no combinatorial explosion)
- Why reporting grids exist (prevent gaps in trend lines, ensure zero-value rows)
- Why pre-computed over calculated (metric governance in one place, no Tableau discrepancies)
- How this mirrors what he built at Attentive (same patterns, different tools)
