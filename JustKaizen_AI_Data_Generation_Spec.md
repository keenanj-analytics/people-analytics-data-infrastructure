# JustKaizen AI - Synthetic Data Generation Spec

This document is the complete input for generating coherent synthetic workforce data across 6 source tables. Every decision the generation script needs to make is defined here. No guessing.

---

## 1. Company Overview and Timeline

**Company:** JustKaizen AI
**What they do:** AI-powered business automation and analytics platform for SMBs in education, social welfare, and nonprofit sectors.
**Mission:** Make powerful technology accessible to organizations that exist to serve others.
**Founded:** 2020, remote-first
**Current state:** 380 active employees, $45M ARR, 30% YoY growth, EBITDA breakeven targeted Q4 2025

### Funding History
- Seed (2020): $4M
- Series A (2021): $20M
- Series B (2023): $80M at ~$400M valuation

### Headcount Timeline

| Period | Event | Start HC | End HC | Net Change |
|--------|-------|----------|--------|------------|
| 2020 Q1-Q4 | Founding | 0 | 12 | +12 |
| 2021 Q1 | Series A ramp begins | 12 | 35 | +23 |
| 2021 Q2 | Aggressive hiring | 35 | 70 | +35 |
| 2021 Q3 | Continued growth | 70 | 120 | +50 |
| 2021 Q4 | Year-end push | 120 | 180 | +60 |
| 2022 Q1 | Hypergrowth begins | 180 | 250 | +70 |
| 2022 Q2 | Peak hiring quarter | 250 | 330 | +80 |
| 2022 Q3 | Growth continues | 330 | 390 | +60 |
| 2022 Q4 | Slowing but still hiring | 390 | 420 | +30 |
| 2023 Q1 | **LAYOFF: 18% RIF** | 420 | 345 | -75 |
| 2023 Q2 | Hiring freeze | 345 | 340 | -5 (attrition only) |
| 2023 Q3 | Series B, cautious rehire | 340 | 350 | +10 |
| 2023 Q4 | Disciplined growth | 350 | 355 | +5 |
| 2024 Q1 | Steady hiring | 355 | 360 | +5 |
| 2024 Q2 | Continued growth | 360 | 367 | +7 |
| 2024 Q3 | Continued growth | 367 | 372 | +5 |
| 2024 Q4 | Year-end hiring | 372 | 380 | +8 |
| 2025 Q1 | Current state | 380 | 380 | 0 (net) |

### 2023 Q1 Layoff Details

75 people terminated as "Layoff / Reduction in Force" in Q1 2023.

| Department | People Cut | Notes |
|------------|-----------|-------|
| Sales | 25 | Mostly SDRs hired for verticals that didn't convert |
| Engineering | 20 | Infrastructure team consolidated, junior roles |
| G&A | 12 | Facilities, internal comms roles eliminated |
| Marketing | 8 | Employer branding eliminated, content reduced |
| Customer Success | 5 | Support team reduced |
| People | 5 | 1 recruiter, 2 L&D, 1 DEIB coordinator, 1 People Ops |
| Product | 0 | Protected from cuts |

---

## 2. Org Structure with Manager Hierarchy

### Leadership Team (M5 - C-Suite)

| employee_id | Name | Title | Department | Reports To | Hire Date |
|-------------|------|-------|------------|-----------|-----------|
| EMP-001 | Maya Chen | CEO | Executive | Board | 2020-01-15 |
| EMP-002 | David Okafor | CTO | Engineering | EMP-001 | 2020-01-15 |
| EMP-003 | Marcus Lee | CRO | Sales | EMP-001 | 2022-02-01 |
| EMP-004 | Aisha Patel | CFO | G&A | EMP-001 | 2021-06-01 |
| EMP-005 | James Wallace | CPO (Product) | Product | EMP-001 | 2023-09-15 |
| EMP-006 | Rachel Torres | Chief People Officer | People | EMP-001 | 2023-10-01 |

Note: Priya Sharma (original CPO/Co-founder) departed Q2 2023. James Wallace replaced her as CPO.

### VP Layer (M4)

| employee_id | Name | Title | Department | Sub-Department | Reports To | Hire Date |
|-------------|------|-------|------------|----------------|-----------|-----------|
| EMP-007 | Kevin Zhao | VP Engineering, Platform | Engineering | Platform | EMP-002 | 2020-06-01 |
| EMP-008 | Amara Johnson | VP Engineering, AI/ML | Engineering | AI/ML | EMP-002 | 2021-03-01 |
| EMP-009 | Carlos Mendez | VP Sales | Sales | All Sales | EMP-003 | 2022-04-01 |
| EMP-010 | Lisa Park | VP Customer Success | Customer Success | All CS | EMP-003 | 2021-09-01 |
| EMP-011 | Nina Okonkwo | VP Marketing | Marketing | All Marketing | EMP-001 | 2021-07-01 |
| EMP-012 | Raj Gupta | VP Finance | G&A | Finance | EMP-004 | 2021-08-01 |

### Director Layer (M3)

| employee_id | Name | Title | Department | Sub-Department | Reports To | Hire Date |
|-------------|------|-------|------------|----------------|-----------|-----------|
| EMP-013 | Sarah Kim | Director of Engineering, Data | Engineering | Data | EMP-002 | 2021-01-15 |
| EMP-014 | Jordan Brooks | Director of Engineering, Infra | Engineering | Infrastructure | EMP-002 | 2021-02-01 |
| EMP-015 | Michelle Torres | Director of Recruiting | People | Recruiting | EMP-006 | 2021-05-01 |
| EMP-016 | Derek Washington | Director of People Ops | People | People Ops | EMP-006 | 2022-01-15 |
| EMP-017 | Hannah Lee | Director of Product | Product | Product Management | EMP-005 | 2021-11-01 |
| EMP-018 | Andre Williams | Director of Sales, Enterprise | Sales | Account Executive | EMP-009 | 2022-05-01 |
| EMP-019 | Priya Sharma | Co-Founder/CPO (DEPARTED) | Product | - | EMP-001 | 2020-01-15 |

Note: EMP-019 (Priya Sharma) termination_date = 2023-06-30, termination_type = Voluntary, termination_reason = Personal Reasons

### Manager Layer (M1-M2)

Generate 50 managers (M1-M2) distributed across departments as follows:

| Department | M1 Count | M2 Count | Notes |
|------------|----------|----------|-------|
| Engineering | 14 | 4 | ~1 manager per 7-8 ICs |
| Sales | 8 | 2 | ~1 manager per 7 ICs, SDR managers have larger teams |
| Customer Success | 4 | 1 | ~1 manager per 7-8 ICs |
| Marketing | 3 | 1 | Small teams |
| Product | 3 | 1 | PM leads, Design lead, UX lead |
| G&A | 3 | 1 | Finance, Legal, IT managers |
| People | 3 | 1 | HRBP lead, Recruiting managers, L&D lead |

Rules for manager generation:
- All M1-M2 managers must have hire_date at least 6 months before becoming a manager
- Most managers were promoted from IC3 or IC4 (internal promotion), about 30% were external hires directly into M1
- Managers must be in the same department as their direct reports
- M1 reports to M2 or M3. M2 reports to M3 or M4.

---

## 3. Headcount Distribution by Department, Sub-Department, and Level

### Current Active Headcount (380 total)

| Department | Sub-Department | IC1 | IC2 | IC3 | IC4 | IC5 | M1 | M2 | M3 | M4 | M5 | Total |
|------------|---------------|-----|-----|-----|-----|-----|----|----|----|----|-----|-------|
| Engineering | Platform | 3 | 8 | 14 | 7 | 2 | 4 | 1 | 0 | 1 | 0 | 40 |
| Engineering | AI/ML | 2 | 6 | 12 | 8 | 3 | 3 | 1 | 0 | 1 | 0 | 36 |
| Engineering | Data | 2 | 5 | 10 | 6 | 1 | 4 | 1 | 1 | 0 | 0 | 30 |
| Engineering | Infrastructure | 3 | 7 | 11 | 5 | 1 | 3 | 1 | 1 | 0 | 0 | 32 |
| Engineering | (CTO) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 1 |
| Sales | SDR | 10 | 6 | 2 | 0 | 0 | 2 | 0 | 0 | 0 | 0 | 20 |
| Sales | Account Executive | 2 | 6 | 10 | 4 | 0 | 3 | 1 | 1 | 0 | 0 | 27 |
| Sales | Account Management | 1 | 3 | 5 | 2 | 0 | 2 | 1 | 0 | 0 | 0 | 14 |
| Sales | Sales Engineering | 0 | 2 | 4 | 2 | 0 | 1 | 0 | 0 | 0 | 0 | 9 |
| Sales | (CRO) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 1 |
| CS | CSM | 2 | 4 | 6 | 3 | 0 | 2 | 0 | 0 | 0 | 0 | 17 |
| CS | Support | 3 | 4 | 3 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 12 |
| CS | Implementation | 1 | 2 | 3 | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 9 |
| CS | (VP CS) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 1 |
| Marketing | Growth | 1 | 2 | 4 | 2 | 0 | 1 | 0 | 0 | 0 | 0 | 10 |
| Marketing | Content | 1 | 2 | 3 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 8 |
| Marketing | Product Marketing | 0 | 1 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 5 |
| Marketing | Brand | 0 | 1 | 2 | 1 | 0 | 0 | 1 | 0 | 0 | 0 | 4 |
| Marketing | (VP Mktg) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 0 | 1 |
| Product | Product Management | 0 | 2 | 4 | 3 | 0 | 1 | 0 | 1 | 0 | 0 | 11 |
| Product | Design | 1 | 2 | 3 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 8 |
| Product | UX Research | 0 | 1 | 2 | 1 | 0 | 1 | 1 | 0 | 0 | 0 | 6 |
| Product | (CPO) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 1 |
| G&A | Finance | 1 | 3 | 4 | 2 | 0 | 1 | 0 | 0 | 1 | 0 | 12 |
| G&A | Legal | 0 | 1 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 5 |
| G&A | IT | 2 | 3 | 3 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 10 |
| G&A | Facilities | 1 | 2 | 2 | 0 | 0 | 0 | 1 | 0 | 0 | 0 | 6 |
| G&A | (CFO) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 1 |
| People | HRBPs | 0 | 1 | 2 | 1 | 0 | 1 | 0 | 0 | 0 | 0 | 5 |
| People | Recruiting | 1 | 2 | 3 | 1 | 0 | 1 | 0 | 1 | 0 | 0 | 9 |
| People | People Ops | 1 | 1 | 2 | 0 | 0 | 1 | 0 | 1 | 0 | 0 | 6 |
| People | Total Rewards | 0 | 1 | 1 | 1 | 0 | 0 | 0 | 0 | 0 | 0 | 3 |
| People | L&D | 1 | 1 | 2 | 0 | 0 | 1 | 0 | 0 | 0 | 0 | 5 |
| People | DEIB | 0 | 1 | 2 | 0 | 0 | 0 | 1 | 0 | 0 | 0 | 4 |
| People | (CPO) | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 1 | 1 |

---

## 4. Hiring Volume by Quarter and Department

Total hires across the company's history. These numbers include people who have since been terminated. The generation script uses these to assign realistic hire_dates.

| Quarter | Eng | Sales | CS | Mktg | Product | G&A | People | Total |
|---------|-----|-------|-----|------|---------|-----|--------|-------|
| 2020 Q1 | 5 | 0 | 0 | 0 | 3 | 2 | 2 | 12 |
| 2020 Q2-Q4 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 |
| 2021 Q1 | 10 | 3 | 2 | 2 | 3 | 2 | 1 | 23 |
| 2021 Q2 | 15 | 6 | 4 | 3 | 3 | 2 | 2 | 35 |
| 2021 Q3 | 22 | 10 | 5 | 4 | 3 | 3 | 3 | 50 |
| 2021 Q4 | 25 | 15 | 6 | 5 | 3 | 3 | 3 | 60 |
| 2022 Q1 | 28 | 18 | 7 | 6 | 4 | 4 | 3 | 70 |
| 2022 Q2 | 30 | 22 | 8 | 7 | 4 | 5 | 4 | 80 |
| 2022 Q3 | 22 | 16 | 6 | 5 | 3 | 4 | 4 | 60 |
| 2022 Q4 | 10 | 8 | 3 | 3 | 2 | 2 | 2 | 30 |
| 2023 Q1 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 (layoff quarter) |
| 2023 Q2 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 (freeze) |
| 2023 Q3 | 4 | 2 | 1 | 1 | 1 | 0 | 1 | 10 |
| 2023 Q4 | 2 | 1 | 1 | 0 | 0 | 1 | 0 | 5 |
| 2024 Q1 | 2 | 1 | 1 | 0 | 1 | 0 | 0 | 5 |
| 2024 Q2 | 3 | 1 | 1 | 1 | 0 | 1 | 0 | 7 |
| 2024 Q3 | 2 | 1 | 1 | 0 | 1 | 0 | 0 | 5 |
| 2024 Q4 | 3 | 2 | 1 | 1 | 0 | 1 | 0 | 8 |
| 2025 Q1 | 3 | 2 | 1 | 1 | 1 | 1 | 1 | 10 |

---

## 5. Archetype Definitions with Distributions

### Archetype Distribution

| # | Archetype | % of Total Employees | ~Count (of 568 total records) | Status |
|---|-----------|---------------------|------------------------------|--------|
| 1 | High-flyer | 15% | 85 | 80% active, 20% terminated (voluntary, career opp) |
| 2 | Steady contributor | 25% | 142 | 85% active, 15% terminated (mixed voluntary) |
| 3 | Early churner | 10% | 57 | 100% terminated (voluntary, <12mo tenure) |
| 4 | Top performer flight risk | 8% | 45 | 100% terminated (voluntary, career opp, 18-24mo stagnation) |
| 5 | Layoff casualty | 13% | 75 | 100% terminated (layoff, Q1 2023) |
| 6 | Performance managed out | 4% | 23 | 100% terminated (involuntary, performance) |
| 7 | Internal mover | 8% | 45 | 90% active, 10% terminated |
| 8 | Manager step-back | 1% | 8 | 100% active |
| 9 | Manager change casualty | 6% | 34 | 100% terminated (voluntary, within 6mo of manager change) |
| 10 | Founder / early employee | 5% | 30 | 97% active (1 departed: Priya Sharma) |
| - | Defined leadership | 5% | 24 | Pre-defined in org chart above |

### Archetype Rules

**Archetype 1: High-flyer**
- Hire period: 2020-2022
- Starting level: IC2 or IC3
- Promotions: 2-3 over tenure
- Performance pattern: Alternating "Exceeds" and "Significantly Exceeds". Never "Meets" for more than 1 consecutive cycle.
- Comp: Above midpoint (compa_ratio 1.05-1.15). 10-15% bump at each promotion. 4-5% annual merit in non-promotion years.
- If terminated (20%): Voluntary, "Career Opportunity". Left after being in role 18+ months without next promotion.
- If active (80%): is_top_performer = TRUE. 40% chance is_critical_talent = TRUE.

**Archetype 2: Steady contributor**
- Hire period: All years
- Starting level: IC1-IC3
- Promotions: 0-1 over tenure
- Performance pattern: Mostly "Meets" (70%), occasional "Exceeds" (25%), rare "Partially Meets" (5%).
- Comp: Near midpoint (compa_ratio 0.95-1.05). 3-4% annual merit. One promotion bump of 8-12% if promoted.
- If terminated (15%): Voluntary, mixed reasons (Compensation 30%, Work-Life Balance 25%, Role Misalignment 20%, Relocation 15%, Personal Reasons 10%).
- If active (85%): is_top_performer = FALSE. is_critical_talent = FALSE.

**Archetype 3: Early churner**
- Hire period: 2022-2024
- Starting level: IC1-IC2
- Promotions: 0
- Performance: 0-1 review cycles completed. If 1 cycle exists, rating is "Meets" or "Partially Meets".
- Tenure at departure: 2-11 months
- Comp: Within band, near or below midpoint (compa_ratio 0.90-1.00).
- Termination: Voluntary. Reasons: Role Misalignment (35%), Company Culture (25%), Compensation (20%), Personal Reasons (20%).

**Archetype 4: Top performer flight risk**
- Hire period: 2021-2022
- Starting level: IC2-IC3
- Promotions: 1 (then stagnated)
- Performance pattern: "Exceeds" or "Significantly Exceeds" consistently. Strong performer who stopped getting promoted.
- Time since last promotion at departure: 18-24 months
- Comp: At or above midpoint (compa_ratio 1.00-1.10).
- Termination: Voluntary, "Career Opportunity". All were is_top_performer = TRUE at time of departure.

**Archetype 5: Layoff casualty**
- Hire period: 2021-2022
- Starting level: IC1-IC3
- Promotions: 0-1
- Performance: Mixed. Not all were poor performers. Distribution: "Meets" 50%, "Exceeds" 20%, "Partially Meets" 20%, "Does Not Meet" 10%.
- Termination: Layoff, "Reduction in Force". termination_date in Q1 2023 (distribute across Jan 15 - Mar 15, 2023).
- Department distribution per layoff table in Section 1.

**Archetype 6: Performance managed out**
- Hire period: 2021-2023
- Starting level: IC1-IC3
- Promotions: 0
- Performance pattern: Declining over 2-3 cycles. Typical: "Meets" -> "Partially Meets" -> "Does Not Meet" or "Meets" -> "Partially Meets" -> "Partially Meets".
- Comp: Below midpoint (compa_ratio 0.88-0.98). No merit increase in final year.
- Termination: Involuntary, "Performance". Departed 1-3 months after final review cycle.

**Archetype 7: Internal mover**
- Hire period: 2020-2023
- Starting level: IC2-IC4
- Promotions: 0-1 (in addition to the lateral move)
- At least 1 lateral transfer or department move in job history
- Performance: Steady "Meets" or "Exceeds". Rarely declines after a move.
- Comp: Near midpoint. Lateral moves typically do not include comp bumps.
- If terminated (10%): Voluntary, mixed reasons. Left 12+ months after the move.
- If active (90%): is_top_performer = FALSE. Valued for versatility.

**Archetype 8: Manager step-back**
- Hire period: 2020-2022
- Starting level: IC3
- Promoted to M1 after 12-18 months
- Stepped back to IC4 within 12 months of M1 promotion
- Performance as M1: "Meets" or "Partially Meets". Performance after step-back: "Meets" or "Exceeds".
- Comp: Retained IC4 band after step-back (no pay cut). Compa_ratio 1.00-1.10.
- All still active. These employees are respected for self-awareness.

**Archetype 9: Manager change casualty**
- Hire period: 2021-2023
- Starting level: IC2-IC4
- Promotions: 0-1
- Performance before manager change: "Meets" or "Exceeds". Stable performer.
- Experienced a manager change (manager_id changed in job history) 3-9 months before departure.
- Termination: Voluntary. Reasons: Manager Relationship (50%), Company Culture (30%), Career Opportunity (20%).
- Departed within 6 months of the manager change.

**Archetype 10: Founder / early employee**
- Hire period: 2020 (all hired in Q1 2020)
- Starting level: IC3-IC4 or M1
- Promotions: 2-4 over 5 years. Now M2+ or IC4-IC5.
- Performance: "Exceeds" or "Significantly Exceeds" consistently.
- Comp: Well above midpoint (compa_ratio 1.10-1.20). Multiple promotion bumps plus generous annual merit.
- 97% active. Only departure: EMP-019 Priya Sharma.
- All active founders: is_critical_talent = TRUE.

---

## 6. Compensation Rules

### Function-Specific Compensation Bands (2025 rates)

Compensation bands vary by job family, not just by level. An IC4 Staff Engineer and an IC4 Staff People Ops Specialist are the same level but different markets.

**Job Families and Market Multipliers:**

| Job Family | Departments | Multiplier vs Base | Rationale |
|------------|------------|-------------------|-----------|
| Technical | Engineering | 1.20-1.25x | AI/ML premium, competitive engineering market |
| Technical-Adjacent | Product, Design, UX | 1.10x | Technical skills required, slightly less competitive than pure engineering |
| Commercial | Sales, Sales Engineering | 1.00x base + OTE | Quota-carrying roles have variable comp on top |
| Commercial-Adjacent | Customer Success, Implementation | 0.90-0.95x | Customer-facing but not quota-carrying |
| Marketing | Marketing | 0.92-1.00x | Varies by sub-function (Growth pays more than Content) |
| Operations | G&A (Finance, IT, Facilities) | 0.80-0.92x | Non-technical operational roles. Exception: Legal at 1.10x |
| People | People / HR | 0.80-0.95x | Varies by sub-function (Total Rewards and HRBPs higher, L&D and Ops lower) |

**CRITICAL: Use the "Ref - Job Architecture" tab in the Data Dictionary workbook for exact band values per department + sub-department + level + title combination.** The tab contains ~90 role-specific entries with exact Band Min, Band Mid, and Band Max values.

When generating comp data, look up the employee's department + sub_department + job_level to find the correct function-specific band rather than using a flat band-by-level approach.

### Historical Band Adjustments

Bands have shifted upward over time to reflect market adjustments:
- 2020: Bands were 15% lower than 2025 rates
- 2021: Bands were 12% lower
- 2022: Bands were 8% lower
- 2023: Bands were 5% lower
- 2024: Bands were 2% lower
- 2025: Current rates (as listed above)

When generating historical comp records, use the adjusted band for the year the comp was set.

### Compensation Change Rules

| Change Type | Typical Increase | Notes |
|-------------|-----------------|-------|
| Promotion | 10-15% | Must land within the new level's band |
| Annual merit (strong performer) | 4-5% | "Exceeds" or "Significantly Exceeds" rating |
| Annual merit (meets expectations) | 3-4% | "Meets" rating |
| Annual merit (below expectations) | 0-1% | "Partially Meets" rating |
| Annual merit (does not meet) | 0% | No increase. Comp frozen. |
| Market adjustment | 5-8% | Applied company-wide in 2023 Q3 post-Series B |
| Lateral transfer | 0% | No comp change for lateral moves |

### Comp Record Timing

- Annual merit reviews happen in January each year (effective_date = Jan 15)
- Promotion comp changes match the promotion effective_date in job history
- Market adjustment: effective_date = 2023-09-01 for all eligible employees
- New hire comp: effective_date = hire_date

---

## 7. Performance Rating Probabilities by Archetype

Each archetype has a probability distribution for each review cycle. The generation script randomly assigns a rating based on these weights, then applies the coherence rules (e.g., must have "Exceeds" or higher before a promotion).

| Archetype | Sig. Exceeds | Exceeds | Meets | Partially Meets | Does Not Meet |
|-----------|-------------|---------|-------|-----------------|---------------|
| High-flyer | 0.30 | 0.55 | 0.15 | 0.00 | 0.00 |
| Steady contributor | 0.02 | 0.23 | 0.65 | 0.08 | 0.02 |
| Early churner | 0.00 | 0.15 | 0.55 | 0.25 | 0.05 |
| Top performer flight risk | 0.25 | 0.60 | 0.15 | 0.00 | 0.00 |
| Layoff casualty | 0.05 | 0.20 | 0.45 | 0.20 | 0.10 |
| Performance managed out (early) | 0.00 | 0.10 | 0.60 | 0.25 | 0.05 |
| Performance managed out (late) | 0.00 | 0.00 | 0.15 | 0.50 | 0.35 |
| Internal mover | 0.05 | 0.35 | 0.55 | 0.05 | 0.00 |
| Manager step-back (as M1) | 0.00 | 0.15 | 0.60 | 0.25 | 0.00 |
| Manager step-back (after return to IC) | 0.05 | 0.40 | 0.50 | 0.05 | 0.00 |
| Manager change casualty | 0.05 | 0.35 | 0.55 | 0.05 | 0.00 |
| Founder / early employee | 0.35 | 0.55 | 0.10 | 0.00 | 0.00 |

### Review Cycle Schedule

- H1 reviews: Completed July 15 each year (covering Jan-Jun)
- H2 reviews: Completed January 15 each year (covering Jul-Dec of prior year)
- First review cycle available: 2020-H2 (completed Jan 15, 2021)
- Employees must have been hired at least 3 months before the cycle end date to be included
- No reviews exist after an employee's termination_date
- Review status: 95% "Completed", 4% "Incomplete", 1% "Exempt" (executives)

---

## 8. Demographic Distributions

### Gender Distribution by Department

| Department | Male | Female | Non-Binary |
|------------|------|--------|------------|
| Engineering | 62% | 33% | 5% |
| Sales | 52% | 45% | 3% |
| Customer Success | 40% | 56% | 4% |
| Marketing | 38% | 58% | 4% |
| Product | 50% | 45% | 5% |
| G&A | 42% | 55% | 3% |
| People | 28% | 68% | 4% |

### Race/Ethnicity Distribution by Department

| Department | White | Asian | Hispanic/Latino | Black | Two or More | Other/Decline |
|------------|-------|-------|-----------------|-------|-------------|---------------|
| Engineering | 35% | 38% | 10% | 7% | 6% | 4% |
| Sales | 48% | 12% | 18% | 12% | 6% | 4% |
| Customer Success | 42% | 15% | 18% | 14% | 7% | 4% |
| Marketing | 45% | 18% | 15% | 10% | 8% | 4% |
| Product | 38% | 32% | 12% | 8% | 6% | 4% |
| G&A | 45% | 18% | 15% | 12% | 6% | 4% |
| People | 35% | 15% | 18% | 20% | 8% | 4% |

### Location Distribution (US only, remote-first)

| Region | % of Employees | Top Cities |
|--------|---------------|------------|
| California | 30% | San Francisco, Los Angeles, San Diego |
| New York | 15% | New York City, Brooklyn |
| Texas | 10% | Austin, Dallas, Houston |
| Washington | 8% | Seattle, Bellevue |
| Colorado | 5% | Denver, Boulder |
| Illinois | 5% | Chicago |
| Massachusetts | 5% | Boston, Cambridge |
| Georgia | 4% | Atlanta |
| North Carolina | 3% | Raleigh, Charlotte |
| Other states | 15% | Distributed across remaining US states |

---

## 9. Recruiting Source Distributions

### Overall Source Mix

| Source | % of All Hires |
|--------|---------------|
| LinkedIn | 32% |
| Referral | 25% |
| Career Page | 15% |
| Job Board | 12% |
| Agency | 8% |
| Event | 5% |
| Internal | 3% |

### Source Mix Variation by Department

| Department | Top Source | Second Source | Notes |
|------------|-----------|-------------|-------|
| Engineering | Referral (35%) | LinkedIn (28%) | Engineers prefer referrals |
| Sales | LinkedIn (40%) | Job Board (20%) | Outbound recruiting heavy |
| Customer Success | Career Page (25%) | LinkedIn (25%) | Even spread |
| Marketing | LinkedIn (30%) | Referral (25%) | Balanced |
| Product | Referral (30%) | LinkedIn (28%) | Similar to engineering |
| G&A | Job Board (28%) | Career Page (25%) | More traditional channels |
| People | LinkedIn (35%) | Referral (25%) | HR community is LinkedIn-heavy |

### Recruiting Funnel Conversion Rates

| Stage Transition | Conversion Rate | Notes |
|-----------------|----------------|-------|
| Applied -> Phone Screen | 25% | Initial screening |
| Phone Screen -> Technical/Panel | 45% | Recruiter qualification |
| Technical/Panel -> Onsite | 55% | Technical bar |
| Onsite -> Offer | 40% | Final decision |
| Offer -> Hired | 85% | Offer acceptance rate |
| Overall: Applied -> Hired | ~2.1% | End-to-end |

### Rejection Reasons

| Stage | Reason | % of Rejections at Stage |
|-------|--------|-------------------------|
| Phone Screen | Not Qualified | 40% |
| Phone Screen | Compensation Mismatch | 25% |
| Phone Screen | Poor Communication | 20% |
| Phone Screen | Withdrew | 15% |
| Technical | Failed Technical | 55% |
| Technical | Withdrew | 25% |
| Technical | Not Qualified | 20% |
| Onsite | Culture Fit | 30% |
| Onsite | Failed Technical | 25% |
| Onsite | Withdrew | 25% |
| Onsite | Went with Another Offer | 20% |
| Offer | Declined - Compensation | 45% |
| Offer | Declined - Accepted Other Offer | 35% |
| Offer | Declined - Personal Reasons | 20% |

---

## 10. Engagement Survey Question Bank

### Survey Structure
- 8 themes, 3-4 questions per theme = 28 questions total
- Response scale: 1 (Strongly Disagree) to 5 (Strongly Agree)
- eNPS question is separate: 0-10 scale
- Surveys run quarterly: Q1 (Mar), Q2 (Jun), Q3 (Sep), Q4 (Dec)
- First survey: 2021-Q2
- Response rate: 78-88% per cycle

### Question Bank

**ENG - Employee Engagement**
- ENG1: "I would recommend this company as a great place to work." (eNPS proxy, DO NOT use as regression predictor)
- ENG2: "I see myself still working here in two years."
- ENG3: "I am proud to tell others I work at this company."
- ENG4: "I feel motivated to go above and beyond what is expected of me." (outcome-adjacent, use with caution)

**MGE - Manager Effectiveness**
- MGE1: "My manager gives me regular and meaningful feedback on my work."
- MGE2: "My manager supports my professional development and career growth."
- MGE3: "My manager keeps me informed about how my work connects to the broader picture."
- MGE4: "My manager creates an environment where I feel comfortable raising concerns."
- MGE5: "My manager treats all team members fairly and equitably."

**CGD - Career Growth & Development**
- CGD1: "I have access to the learning and development opportunities I need to grow in my career."
- CGD2: "I can see a clear path for my career progression at this company."
- CGD3: "The work I do is meaningful and contributes to my professional growth."

**WLB - Work-Life Balance**
- WLB1: "I am able to maintain a healthy balance between my work and personal life."
- WLB2: "My workload is manageable and sustainable."
- WLB3: "I feel supported when I need to take time off for personal reasons."

**REC - Recognition**
- REC1: "I receive adequate recognition when I do good work."
- REC2: "Recognition at this company is given fairly and consistently."
- REC3: "My contributions are valued by my team and leadership."

**CUL - Company Culture**
- CUL1: "I feel a strong sense of belonging at this company."
- CUL2: "The company's values are reflected in how we operate day-to-day."
- CUL3: "I trust the leadership team to make good decisions for the company."

**COM - Communication**
- COM1: "I receive clear and timely communication about company strategy and direction."
- COM2: "I understand how my work contributes to the company's goals."
- COM3: "Cross-team communication and collaboration is effective."

**RES - Resources & Enablement**
- RES1: "I have access to the tools and technology I need to do my job effectively."
- RES2: "I have the information and context I need to make good decisions in my role."
- RES3: "Our team processes and workflows enable me to do my best work."

### Engagement Score Generation Rules

Department-level scores with realistic patterns:

**Baseline Scores (company-wide average, 2025-Q1):**

| Theme | Avg Score | Favorable % | Notes |
|-------|-----------|-------------|-------|
| ENG | 3.7 | 68% | Below benchmark (~3.9) |
| MGE | 3.9 | 74% | Relatively strong |
| CGD | 3.4 | 55% | Weakest area |
| WLB | 3.5 | 58% | Post-layoff stress |
| REC | 3.3 | 50% | Biggest gap |
| CUL | 3.8 | 72% | Mission-driven boost |
| COM | 3.5 | 56% | Common scaling challenge |
| RES | 3.7 | 66% | Adequate but not strong |

**Department Variations from Baseline:**

| Department | Higher Than Baseline | Lower Than Baseline |
|------------|---------------------|---------------------|
| Engineering | RES (+0.3), CUL (+0.2) | WLB (-0.3), REC (-0.2) |
| Sales | ENG (+0.1) | WLB (-0.4), CGD (-0.3), REC (-0.3) |
| Customer Success | CUL (+0.3), MGE (+0.2) | COM (-0.2) |
| Marketing | CUL (+0.2), REC (+0.2) | RES (-0.2) |
| Product | CGD (+0.3), RES (+0.3) | COM (-0.2) |
| G&A | WLB (+0.2) | CGD (-0.2), ENG (-0.1) |
| People | CUL (+0.3), MGE (+0.2) | WLB (-0.2), RES (-0.2) |

**Time Trends:**
- 2021-Q2 to 2022-Q2: Scores trending up (growth optimism)
- 2022-Q3 to 2023-Q1: Scores dropping (pre-layoff anxiety)
- 2023-Q1 to 2023-Q3: Sharp drop (post-layoff trauma). All themes -0.2 to -0.4 from pre-layoff.
- 2023-Q4 to 2024-Q2: Gradual recovery
- 2024-Q3 to 2025-Q1: Stabilized near but slightly below pre-layoff levels

**eNPS by Department (2025-Q1):**

| Department | eNPS |
|------------|------|
| Engineering | 25 |
| Sales | 15 |
| Customer Success | 35 |
| Marketing | 30 |
| Product | 40 |
| G&A | 20 |
| People | 28 |
| Company-wide | 27 |

---

## 11. Data Dictionary Summary

Six raw tables to generate:

1. **raw_employees** (ADP): ~568 rows (380 active + ~188 terminated). Core HR record.
2. **raw_job_history** (ADP): ~800-1,000 rows. One row per change event per employee. Every employee has at least one row (Hire event).
3. **raw_compensation** (Pave): ~900-1,100 rows. One row per comp change. Every employee has at least one row (starting comp).
4. **raw_recruiting** (Ashby): ~8,000-10,000 rows. Every hire produces ~20 candidate applications (2.1% end-to-end conversion). Generate applications for all hires plus rejected/withdrawn candidates.
5. **raw_performance** (Lattice): ~2,500-3,000 rows. One row per employee per review cycle for all cycles during their tenure.
6. **raw_engagement** (Lattice): ~3,500-4,000 rows. 28 questions x 7 departments x ~16 survey cycles. Sub-department rows only where 5+ respondents.

---

## 12. Cross-Table Coherence Rules

These rules MUST be validated after data generation. If any rule is violated, the data is broken.

### Employee <-> Recruiting
- Every hired candidate in Ashby must have a matching employee in ADP with the same hire_date
- The Ashby job_title must match the new_job_title on the "Hire" row in job_history
- The Ashby department must match the new_department on the "Hire" row in job_history
- Ashby application_date must be before hire_date
- Not every employee needs an Ashby record (founders and very early hires may predate the ATS)

### Employee <-> Job History
- Every employee must have at least one job_history row with change_type = "Hire" and effective_date = hire_date
- The Hire row's new_job_level, new_department, new_job_title must match the employee's starting state
- The most recent job_history row's new_job_level, new_department, new_job_title must match the employee's current state in the employees table
- No job_history events after termination_date
- Promotion events require the prior review cycle rating to be "Exceeds" or "Significantly Exceeds"
- Job level progression must be sequential (IC2 -> IC3, not IC2 -> IC5)

### Employee <-> Compensation
- Every employee must have at least one Pave record with effective_date = hire_date
- Starting salary must fall within the comp band for their starting level (adjusted for the hire year)
- Promotion comp changes must: have effective_date matching the promotion event in job_history, increase salary 10-15%, and land within the new level's band
- Annual merit effective_dates should be Jan 15 of each year during the employee's tenure
- No comp records after termination_date
- Salary in the employees table (if we store current salary there) must match the most recent Pave record

### Employee <-> Performance
- No performance reviews before the employee's hire_date + 3 months
- No performance reviews after the employee's termination_date
- Review cycles must follow the H1/H2 schedule (July 15 and Jan 15)
- Employees terminated for performance must show declining ratings in their last 2-3 cycles
- Employees who were promoted must show "Exceeds" or "Significantly Exceeds" in the cycle immediately before the promotion
- Founder/early employees (hired 2020) should not have reviews before 2020-H2

### Employee <-> Engagement
- No direct join (anonymized). Coherence is at the department level.
- Response counts per department per cycle should not exceed the active headcount in that department at the time of the survey
- Response counts should reflect 78-88% response rate
- Sub-department data suppressed when fewer than 5 respondents

### Job History <-> Compensation
- Every promotion in job_history must have a corresponding comp increase in Pave with the same effective_date
- Lateral transfers in job_history should NOT have a corresponding comp change
- Manager changes in job_history should NOT have a corresponding comp change

### Temporal Consistency
- All dates must be chronologically ordered per employee
- hire_date < first job_history event (Hire) effective_date (they should be equal)
- Each subsequent job_history event must have a later effective_date than the previous
- Each Pave record must have a later effective_date than the previous for the same employee
- termination_date must be after all other events for that employee
- No future dates (all dates <= 2025-03-31, the current quarter)
