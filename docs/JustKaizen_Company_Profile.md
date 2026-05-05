# JustKaizen AI: Company Profile

## The Company

**JustKaizen AI** is a fictional 1,200-person enterprise AI company preparing for IPO. The company builds intelligent automation that helps mid-market and enterprise organizations streamline business operations, analytics, and decision-making workflows.

Founded in 2018 as a remote-first startup (HQ in San Francisco), JustKaizen grew from a 15-person team to over 1,200 employees in under six years — fueled by five rounds of venture funding totaling $360M at a $2.5B valuation.

---

## The Growth Story

JustKaizen's trajectory mirrors the real-world pattern many high-growth tech companies followed between 2018 and 2025:

**2018–2019 — Early Stage.** Small team, first enterprise pilots, product-market fit. Headcount grew from 15 to 80.

**2020–2021 — Hypergrowth.** Series B and C funding ($180M combined). COVID accelerated demand for automation. Headcount surged from 80 to 700. Hiring velocity peaked at 250 new employees in a single half-year.

**2022 — Peak and Correction.** Headcount hit 1,200, but market conditions shifted. Growth slowed. The board began pushing for a path to profitability.

**2023 Q1 — The Layoff.** JustKaizen conducted a 12% reduction in force, cutting 150 employees across every department. Engineering, Sales, and Marketing were hit hardest. A hiring freeze followed through Q2.

**2023 Q3–2024 — Recovery.** Series D funding ($150M) stabilized the company. Disciplined hiring resumed. By Q4 2024, headcount returned to pre-layoff levels.

**2025 — Present Day.** 1,200 active employees. ~$180M ARR. 25% year-over-year growth. IPO targeted for 2026.

---

## The Organization

JustKaizen operates across 8 departments with a standardized level framework spanning individual contributors (P1–P6), managers (M1–M4), and senior leadership (E1–E6).

| Department | Headcount | % of Company |
|------------|-----------|--------------|
| Engineering | 410 | 34% |
| Sales | 200 | 17% |
| Customer Success | 130 | 11% |
| G&A (Finance, Legal, IT, Ops) | 110 | 9% |
| Marketing | 100 | 8% |
| Product | 100 | 8% |
| People | 80 | 7% |
| Data & Analytics | 55 | 5% |
| Executive | 15 | 1% |

---

## The Data Problem

Like many companies that scaled fast, JustKaizen's people data is scattered across disconnected systems — HRIS, compensation, performance management, recruiting, and engagement surveys. Each system has its own format, its own update cadence, and its own version of the truth.

Leadership needs answers: Where is attrition spiking? Are we paying equitably? Which recruiting channels produce the best hires? Are our top performers being retained? How did the layoff affect engagement?

No one can answer these questions reliably because no single source of truth exists.

**That's the problem this project solves.**

---

## The Data

The project uses synthetic employee data modeled after real-world HR systems. The dataset covers January 2020 through March 2025 (63 months) and includes approximately 1,900 total employees (1,200 currently active + ~700 terminated across the data scope).

The raw data spans six source systems:

| Source | What It Contains |
|--------|-----------------|
| HRIS (Employees) | Employee demographics, job details, hire/termination dates, employment status |
| Compensation | Salary history, compensation bands, compa-ratios |
| Performance | Semi-annual review ratings, top performer flags |
| Job History | Promotions, transfers, department changes |
| Recruiting | Requisitions, candidates, pipeline stages, offers, hires |
| Engagement Surveys | Semi-annual survey responses across 27 questions and 8 themes |

---

## What Gets Built

This data feeds into the **[People Analytics Data Infrastructure](https://github.com/keenanj-analytics/people-analytics-data-infrastructure)** — a production-grade dbt + BigQuery data warehouse with 25 models that consolidate, clean, and transform the raw data into reporting-ready marts covering:

- **Attrition** — TTM attrition rates by segment with org-wide and department benchmarks
- **Compensation** — Band positioning, compa-ratios, and equity analysis
- **Recruiting** — Pipeline velocity, source effectiveness, and time-to-fill
- **Headcount** — Monthly roster with demographic and tenure breakdowns
- **Performance** — Rating distributions and top performer identification
- **DEI** — Representation across gender and race/ethnicity by department and level
- **Employee Detail** — Row-level drill-through for every active and terminated employee
- **Engagement** — Survey trends by theme, department, and demographic segment

---

## Why It Matters

This isn't a tutorial or a toy dataset. It's a realistic simulation of the data infrastructure challenge that every scaling company faces — and a demonstration of how to solve it with modern analytics engineering practices (dbt, version control, testing, documentation, and layered transformation design).

For the full technical spec, architecture documentation, and data dictionary, explore the [repository](https://github.com/keenanj-analytics/people-analytics-data-infrastructure).
