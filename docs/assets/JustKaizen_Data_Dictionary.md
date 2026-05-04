# JustKaizen AI: Data Dictionary (V1 - Final)

This document defines every field in every model. Matches the final ERD as of this session.

---

## Layer 1: Raw Source Tables

### raw_employees

**Source:** ADP HRIS export (combined active + terminated in single file)
**Grain:** One row per employee as of Report_Date. In production, multiple monthly snapshots are UNION'd to create a time series.
**PK:** Work_Email
**FK:** Requisition_ID (links to raw_offers_hires)

| Field | Type | Description |
|-------|------|-------------|
| Work_Email | STRING | PK. Lowercase work email. Pattern: {first_initial}{last}@justkaizen.com |
| Requisition_ID | STRING | FK. Links to raw_offers_hires. Pattern: "R-{dept_code}-1001-JustKaizen-{seq}" |
| Position_ID | STRING | ADP position identifier. Pattern: "JK100000" |
| Report_Date | DATE | Snapshot date for this export (end of month) |
| Employment_Status | STRING | "Active" or "Terminated" |
| Full_Name | STRING | "First Last" |
| First_Name | STRING | Legal first name |
| Last_Name | STRING | Legal last name |
| Hire_Date | STRING | Original hire date (MM/DD/YYYY format from ADP, needs casting) |
| Work_Country | STRING | "UNITED STATES" (~95%) or international |
| Work_City | STRING | City name |
| Work_State | STRING | US state abbreviation |
| Pay_Zone | STRING | "ZONE A" (SF/NYC metro) or "ZONE B" (everywhere else). Determines which comp band applies. |
| Gender | STRING | ADP values: "Men", "Women", "Non-Binary", "Not Specified" |
| Race | STRING | "White", "Asian", "Hispanic or Latino", "Black or African American", "Two or More Races", "N/A - EU / APAC", "Not Specified" |
| Manager_Email | STRING | Work email of direct manager. Join key for building org hierarchy. |
| Manage_Name | STRING | Manager's full name |
| Department | STRING | Department name |
| Sub_Department | STRING | Sub-department (cost center grouping) |
| Team | STRING | Team within sub-department. Prefix pattern: "Eng - Platform", "Rev - SDRs", "Mktg - Growth" |
| Job_Title | STRING | Full job title. Also used as join key to raw_comp_bands (Title field). |
| Job_Level | STRING | Level framework: P1-P6 (IC), M1-M4 (Manager/Director), E1-E6 (Senior Leadership) |
| Employment_Type | STRING | "Full Time", "Part Time", "Contractor". Filter to "Full Time" for all metrics. |
| Termination_Date | DATE | NULL for active employees. Populated for terminated. |
| Termination_Reason | STRING | Specific reason: "Career Opportunities", "Compensation", "Work-Life Balance", "Manager Relationship", "Personal Reasons", "Reduction in Force", etc. NULL for active. |
| Termination_Regrettable | STRING | People Ops designation: "Regrettable" or "Nonregrettable". Independent of performance. NULL for active. |
| Termination_Voluntary | BOOL | TRUE = voluntary departure, FALSE = involuntary (including RIF). NULL for active. RIF is identified by Termination_Reason = "Reduction in Force". |
| No_Direct_Reports | INT | Count of direct reports. 0 for ICs. |
| No_Indirect_Reports | INT | Count of skip-level reports. 0 for most. |
| Manager_Status | BOOL | TRUE if the employee manages people. |
| Tenure_bucket | STRING | Pre-computed in ADP. "0-6 Months", "6-12 Months", etc. Note: dbt will recompute this using 1-year intervals for reporting. |
| Critical_Talent | BOOL | People Ops designation. ~5% of population. |
| Hire_Origin | STRING | Usually NULL. Populated if tracked in ADP. |
| Hire_Recruiter | STRING | Recruiter credited with the hire (from ADP). |

### raw_performance

**Source:** Lattice performance review export
**Grain:** One row per (Question_ID x Reviewee_Email x Cycle_Name). Very granular. Includes self reviews, peer reviews, manager reviews, and multiple questions per review.
**PK:** (Question_ID, Reviewee_Email, Cycle_Name) composite
**FK:** Reviewee_Email (links to raw_employees.Work_Email)

| Field | Type | Description |
|-------|------|-------------|
| Question_ID | STRING | PK (composite). Unique question identifier |
| Reviewee_Email | STRING | PK/FK. Employee being reviewed. Links to raw_employees.Work_Email |
| Cycle_Name | STRING | PK (composite). "2024 Year-End Review Cycle", "2024 Mid-Year Review Cycle", etc. |
| Reviewer_Name | STRING | Name of the person writing the review |
| Reviewer_email | STRING | Email of the reviewer |
| Reviewee_Name | STRING | Name of the employee being reviewed |
| Question | STRING | Question text. The rating lives on the "Performance Category" question. |
| Score | STRING | Raw score from reviewer (1-4 scale at source, or " --" for text-only questions) |
| Score_Description | STRING | Rating text: "1 - Truly Outstanding", "2 - Frequently Exceeds Expectations", "3 - Strong Contributor", "4 - Does Not Meet Expectations" |
| Calibrated_Score | STRING | Post-calibration score. Often " --" if calibration didn't override. |
| Calibrated_Score_Description | STRING | Post-calibration rating text |
| Response_Text | STRING | Free-text response for open-ended questions. " --" for rating questions. |
| Response_Type | STRING | "manager", "self", "peer". **Filter to "manager" for official ratings.** |

**Staging filter:** stg_performance filters to `Response_Type = 'manager' AND Question = 'Performance Category'` to produce one official rating per employee per cycle.

### raw_offers_hires

**Source:** ATS (recruiting pipeline) export
**Grain:** One row per candidate-job combination. Includes all candidates (hired, rejected, declined).
**PK:** Candidate_ID
**FK:** Requisition_ID (links to raw_employees.Requisition_ID)

| Field | Type | Description |
|-------|------|-------------|
| Candidate_ID | STRING | PK. Unique candidate-job identifier |
| Requisition_ID | STRING | FK. Links to raw_employees.Requisition_ID. Pattern: "R-{code}-1001-JustKaizen-{seq}" |
| Requisition_Fill_Start_Date | DATE | When the requisition was opened |
| Outcome | STRING | "Hired" or "Archived" |
| Job | STRING | Job title on the requisition |
| Job_Status | STRING | "closed" or "open" |
| Recruiter | STRING | Recruiter who owned this candidate. May differ from Hire_Recruiter in raw_employees. |
| Hiring_Manager | STRING | Hiring manager on the requisition |
| Origin | STRING | "sourced", "applied", "referred", "agency", "internal" |
| Source | STRING | Specific source: "LinkedIn", "Referral", "Career Page", "Job Board", "Agency", "Glassdoor" |
| Current_Interview_Stage | STRING | Current stage: "Hired", "Archived", or stage name |
| Furthest_Stage_Reached | STRING | "Applied", "Recruiter Screen", "Phone Screen", "Technical", "Onsite", "Offer", "Hired" |
| Archive_Reason | STRING | NULL for hired. "Not qualified", "Position filled", "Offer declined", etc. |
| Offer_Decline_Category | STRING | "Compensation", "Accepted Other Offer", "Location", etc. NULL if not declined. |
| Candidate_Application_Date | DATE | When candidate entered the pipeline |
| Candidate_Stage_1_Interview_Date | DATE | First interview date |
| Candidate_Stage_2_Interview_Date | DATE | Second interview date |
| Candidate_Offer_Stage_Entered_Date | DATE | When offer was extended |
| Candidate_Offer_Accept_Date | DATE | When offer was accepted. NULL if declined or not reached. |
| Candidate_Start_Date | DATE | Actual or anticipated start date |

**Key derived metrics (computed in dbt, not in raw):**
- Time_to_Fill = DATE_DIFF(Candidate_Start_Date, Requisition_Fill_Start_Date, DAY)
- Time_to_Hire = DATE_DIFF(Candidate_Start_Date, Candidate_Application_Date, DAY)
- Offer_Accepted = TRUE if Outcome = 'Hired' AND Candidate_Offer_Accept_Date IS NOT NULL

### raw_ees_responses

**Source:** Employee Engagement Survey platform export
**Grain:** One row per (Anonymized_User_ID x EES_Question x EES_Cycle)
**PK:** Anonymized_User_ID (per question per cycle)
**Relationship to raw_employees:** Anonymized. No direct join key. Conceptual link only.

| Field | Type | Description |
|-------|------|-------------|
| Anonymized_User_ID | STRING | Hashed identifier. Cannot be traced back to Work_Email. |
| EES_Cycle | STRING | "2024 H2 Engagement Survey", "2025 H1 Engagement Survey", etc. |
| EES_Submission_Date | STRING | Date the employee submitted their response |
| EES_Theme_Name | STRING | Theme: "Employee Engagement", "Manager Effectiveness", "Career Growth & Development", "Work-Life Balance", "Recognition", "Culture & Values", "Communication & Transparency", "Resources & Enablement" |
| EES_Question | STRING | Full question text |
| eNPS | INT | 0-10 scale. Only populated for the eNPS question. NULL for Likert questions. |
| enps_Category | STRING | "Promoter" (9-10), "Passive" (7-8), "Detractor" (0-6). NULL for non-eNPS questions. |
| Response_Likert | INT | 1-5 scale. Only populated for Likert questions. NULL for eNPS question. |
| Department | STRING | Respondent's department |
| Sub_Department | STRING | Respondent's sub-department |
| Tenure_Bucket | STRING | Respondent's tenure range |
| Team | STRING | Respondent's team |
| Radford_Level | STRING | Respondent's job level (P1-P6, M1-M4, E1-E6) |
| Is_A_Manager | BOOL | Whether the respondent manages people |
| Is_Top_Performer | BOOL | Whether the respondent is flagged as a top performer |

**Note:** Despite individual-level demographic fields, the data is anonymized (no employee_id). MVP dashboard aggregates to department level. Phase 2 can slice by sub-department, level, tenure, and manager status since response counts are large enough at those grains.

### raw_comp_bands

**Source:** Total Rewards / Compensation team export
**Grain:** One row per job title
**PK:** Title

| Field | Type | Description |
|-------|------|-------------|
| Title | STRING | PK. Job title. Join key to raw_employees.Job_Title. |
| Department | STRING | Department this title belongs to |
| Job_Family | STRING | Job family grouping (e.g., "Software Engineering", "Sales Development") |
| Job_Code | STRING | Internal job code (e.g., "JKEN0010") |
| Level | STRING | Job level (P1-P6, M1-M4, E1-E6) |
| Zone_A_Min_Salary | FLOAT | Zone A (SF/NYC metro) band minimum |
| Zone_A_Mid_Salary | FLOAT | Zone A band midpoint. Used for compa-ratio denominator. |
| Zone_A_Max_Salary | FLOAT | Zone A band maximum |
| Zone_B_Min_Salary | FLOAT | Zone B (everywhere else) band minimum |
| Zone_B_Mid_Salary | FLOAT | Zone B band midpoint |
| Zone_B_Max_Salary | FLOAT | Zone B band maximum |

**Compa-ratio formula:** `ROUND(salary / band_mid, 2)` where band_mid is Zone_A_Mid or Zone_B_Mid based on the employee's Pay_Zone.

---

## Layer 2: Staging Models

The staging layer cleans field names, casts data types, and applies source-level filters. No joins. No business logic.

### stg_employees

**Source:** raw_employees
**Grain:** Same as source (one row per employee per report_date)
**Changes from raw:**
- Cast Hire_Date from string to DATE
- Cast Termination_Date from string to DATE
- Map Manager_Email to manager_id (employee_id of the manager)
- Rename Pay_Zone to employee_zone
- Standardize field naming to snake_case

### stg_performance

**Source:** raw_performance
**Grain:** One row per employee per review cycle (FILTERED from source grain)
**Critical filter:** `Response_Type = 'manager' AND Question = 'Performance Category'`
**Changes from raw:**
- Filters out self reviews, peer reviews, and text-only questions
- Maps Reviewee_Email to employee_id
- Maps Score to overall_rating_numeric (inverted: source 1=best becomes target 5=best)
- Adds overall_rating as clean description ("Exceeds Expectations" without the number prefix)

**Rating mapping (source to target):**

| Source Score | Source Description | Target Numeric | Target Description |
|------------|-------------------|---------------|-------------------|
| 1 | 1 - Truly Outstanding | 5 | Significantly Exceeds Expectations |
| 2 | 2 - Frequently Exceeds Expectations | 4 | Exceeds Expectations |
| 3 | 3 - Strong Contributor | 3 | Meets Expectations |
| 4 | 4 - Does Not Meet Expectations | 1 | Does Not Meet Expectations |

**Note:** Source uses 1-4 scale where 1 is best. Target uses 1-5 scale where 5 is best. The 2-point rating (Partially Meets) does not exist in the source Lattice system; it will be generated in synthetic data for JustKaizen.

### stg_recruiting

**Source:** raw_offers_hires
**Grain:** Same as source (one row per candidate-job)
**Changes from raw:**
- Rename verbose field names to clean snake_case
- Add employee_id for hired candidates (joined via Requisition_ID to raw_employees)
- Add offer_accepted BOOL (TRUE if Outcome = 'Hired' AND Candidate_Offer_Accept_Date IS NOT NULL)
- Compute time_to_fill_days = DATE_DIFF(Candidate_Start_Date, Requisition_Fill_Start_Date, DAY)
- Compute time_to_hire_days = DATE_DIFF(Candidate_Start_Date, Candidate_Application_Date, DAY)
- Rename Source to application_channel (BigQuery reserved word)

### stg_engagement

**Source:** raw_ees_responses
**Grain:** Same as source (one row per anonymized user per question per cycle)
**Changes from raw:**
- Clean field names to snake_case
- No employee_id join (anonymized data)

### stg_comp_bands

**Source:** raw_comp_bands
**Grain:** Same as source (one row per title)
**Changes from raw:**
- Parse salary strings from "$174,000" format to numeric FLOAT
- Clean field names to snake_case

---

## Layer 3: Intermediate Models

### dim_calendar

**Type:** dbt model (generated via GENERATE_DATE_ARRAY)
**Grain:** One row per day
**Range:** January 1, 2020 through December 31, 2026 (~2,557 rows)

| Field | Type | Description |
|-------|------|-------------|
| calendar_date | DATE | Every day in range |
| report_month | DATE | DATE_TRUNC(calendar_date, MONTH) |
| report_quarter | STRING | "2025 Q1" format |
| report_year | INT | Calendar year |
| is_month_end | BOOL | TRUE if last day of month |
| is_quarter_end | BOOL | TRUE if is_month_end AND month in (3,6,9,12) |
| flag_latest_month | BOOL | TRUE if report_month = MAX(report_month) with data |

### int_employee_monthly_roster

**Type:** Intermediate view
**Grain:** One row per employee per month they were active (or terminated in that month)
**Sources:** stg_employees, dim_calendar, stg_performance, stg_recruiting, stg_comp_bands
**Row count estimate:** ~65,000-75,000

| Field | Type | Source / Logic |
|-------|------|---------------|
| employee_id | STRING | Derived from stg_employees (Work_Email or Position_ID) |
| report_month | DATE | dim_calendar (distinct report_months) |
| report_quarter | STRING | dim_calendar |
| full_name | STRING | stg_employees |
| email | STRING | stg_employees.Work_Email |
| department | STRING | stg_employees (point-in-time if job history tracked) |
| sub_department | STRING | stg_employees |
| team | STRING | stg_employees |
| job_title | STRING | stg_employees |
| job_level | STRING | stg_employees (P1-P6, M1-M4, E1-E6) |
| level_group | STRING | CASE: P1-P3="Junior IC", P4-P6="Senior IC", M1-M2="Manager", M3-M4="Director", E1-E6="Senior Leadership" |
| employee_zone | STRING | stg_employees.Pay_Zone ("ZONE A" or "ZONE B") |
| manager_id | STRING | Mapped from stg_employees.Manager_Email |
| hire_date | DATE | stg_employees |
| termination_date | DATE | stg_employees (NULL for active) |
| termination_type | STRING | Derived: "Voluntary" if Termination_Voluntary=TRUE, "Involuntary" if FALSE, NULL if active |
| termination_reason | STRING | stg_employees |
| is_regrettable_termination | STRING | stg_employees.Termination_Regrettable ("Regrettable"/"Nonregrettable") |
| employment_status | STRING | "Active" if report_month < termination_month, "Terminated" if report_month = termination_month |
| tenure_months | INT | DATE_DIFF(end of report_month, hire_date, MONTH) |
| tenure_bucket | STRING | 1-year intervals: "0-1 Years", "1-2 Years", "2-3 Years", "3-4 Years", "4-5 Years", "5+ Years" |
| new_hire_flag | STRING | "New Hire" if tenure <= 12 months, "Tenured" otherwise |
| gender | STRING | stg_employees ("Men", "Women", "Non-Binary", "Not Specified") |
| race_ethnicity | STRING | stg_employees |
| location_state | STRING | stg_employees.Work_State |
| location_country | STRING | stg_employees.Work_Country |
| is_critical_talent | BOOL | stg_employees.Critical_Talent |
| employment_type | STRING | stg_employees.Employment_Type ("Full Time" for all metrics) |
| salary | FLOAT | stg_employees (current salary, or from comp history if tracked) |
| comp_band_min | FLOAT | stg_comp_bands, selected Zone A or B based on employee_zone |
| comp_band_mid | FLOAT | stg_comp_bands |
| comp_band_max | FLOAT | stg_comp_bands |
| compa_ratio | FLOAT | ROUND(salary / comp_band_mid, 2) |
| latest_perf_rating | STRING | stg_performance (most recent completed review as of report_month) |
| latest_perf_rating_numeric | INT | Mapped: 5=Sig Exceeds, 4=Exceeds, 3=Meets, 2=Partially Meets, 1=Does Not Meet |
| top_performer_flag | STRING | "Y" if latest_perf_rating_numeric >= 4 OR is_critical_talent = TRUE, else "N" |
| candidate_source | STRING | stg_recruiting (application_channel for hired candidate) |
| candidate_origin | STRING | stg_recruiting (Origin field: applied/sourced/referred/agency/internal) |
| candidate_recruiter | STRING | stg_recruiting (Recruiter for the candidate pipeline) |
| candidate_hiring_manager | STRING | stg_recruiting (Hiring_Manager on the req) |
| no_direct_reports | INT | COUNT of employees where manager_id = this employee in same month |
| manager_status | BOOL | stg_employees.Manager_Status |
| flag_latest_report | STRING | "X" if report_month = MAX(report_month) across table |
| is_terminated_this_month | BOOL | DATE_TRUNC(termination_date, MONTH) = report_month |
| is_excluded_termination | BOOL | TRUE if termination_reason IN ('Reduction in Force', 'End of Contract', 'Entity Change', 'Acquisition/Merger', 'End of Internship', 'International Transfer', 'Relocation', 'Converting to FT'). This is a data cleaning convention, not a dashboard filter. |
| is_attrition_eligible_term | BOOL | is_terminated_this_month = TRUE AND is_excluded_termination = FALSE |

### int_reporting_grid_attrition

**Grain:** report_month x department x sub_department x job_level x level_group x tenure_bucket x top_performer_flag x gender x race_ethnicity
**Source:** dim_calendar CROSS JOIN distinct dimension combos from roster

### int_reporting_grid_recruiting

**Grain:** report_month x department x sub_department x job_level x candidate_source x candidate_origin x candidate_recruiter x candidate_hiring_manager
**Source:** dim_calendar CROSS JOIN distinct dimension combos from recruiting data

### int_reporting_grid_workforce

**Grain:** report_month x department x sub_department x job_level x level_group x gender x race_ethnicity x manager_status
**Source:** dim_calendar CROSS JOIN distinct dimension combos from roster

### int_reporting_grid_compensation

**Grain:** report_month x department x sub_department x job_level x level_group x gender x latest_performance_rating
**Source:** dim_calendar CROSS JOIN distinct dimension combos from roster

---

## Layer 4: Mart Models

### fct_attrition_reporting

**Grain:** One row per (report_month x dimension combo from attrition grid)
**Row count estimate:** ~15,000-25,000

| Field | Type | Description |
|-------|------|-------------|
| report_month | DATE | From grid |
| report_quarter | STRING | "2024 Q4" |
| flag_end_of_quarter | BOOL | Month in (3,6,9,12) or latest month |
| flag_latest_report | BOOL | report_month = MAX(report_month) |
| department | STRING | Dimension |
| sub_department | STRING | Dimension |
| job_level | STRING | Dimension |
| level_group | STRING | Dimension |
| tenure_bucket | STRING | Dimension |
| top_performer_flag | STRING | Dimension |
| gender | STRING | Dimension |
| race_ethnicity | STRING | Dimension |
| end_month_headcount | INT | Active employees at end of month |
| total_terminations | INT | Attrition-eligible terms |
| voluntary_terminations | INT | Where termination_type = "Voluntary" AND eligible |
| involuntary_terminations | INT | Where termination_type = "Involuntary" AND eligible |
| top_performer_terminations | INT | Where top_performer_flag = "Y" AND eligible |
| regrettable_terminations | INT | Where is_regrettable = "Regrettable" AND eligible |
| ttm_total_terminations | INT | SUM over 12-month window |
| ttm_voluntary_terminations | INT | SUM over 12-month window |
| ttm_avg_headcount | FLOAT | AVG(end_month_headcount) over 12-month window |
| ttm_overall_attrition_rate | FLOAT | ttm_total_terminations / ttm_avg_headcount |
| ttm_voluntary_attrition_rate | FLOAT | ttm_voluntary / ttm_avg_headcount |
| ttm_top_performer_attrition_rate | FLOAT | ttm_top_performer / ttm_avg_headcount |
| ttm_regrettable_attrition_rate | FLOAT | ttm_regrettable / ttm_avg_headcount |
| orgwide_ttm_overall_attrition_rate | FLOAT | Company-wide TTM attrition, same month |
| orgwide_ttm_voluntary_attrition_rate | FLOAT | Company-wide TTM voluntary |
| dept_ttm_overall_attrition_rate | FLOAT | Department-level TTM attrition |

### fct_recruiting_reporting

**Grain:** One row per (report_month x dimension combo from recruiting grid)
**Row count estimate:** ~5,000-10,000

| Field | Type | Description |
|-------|------|-------------|
| report_month | DATE | From grid |
| report_quarter | STRING | |
| flag_end_of_quarter | BOOL | |
| flag_latest_report | BOOL | |
| department | STRING | Dimension |
| sub_department | STRING | Dimension |
| job_level | STRING | Dimension |
| candidate_source | STRING | Dimension |
| candidate_origin | STRING | Dimension |
| candidate_recruiter | STRING | Dimension |
| candidate_hiring_manager | STRING | Dimension |
| total_hires | INT | Hires this month |
| total_offers_extended | INT | Offers (accepted + declined) |
| total_offers_accepted | INT | Accepted |
| total_offers_declined | INT | Declined |
| sum_time_to_fill | FLOAT | SUM of days to fill |
| avg_time_to_fill | FLOAT | sum / count |
| ttm_total_hires | INT | Rolling 12mo sum |
| ttm_offers_extended | INT | Rolling 12mo sum |
| ttm_offers_accepted | INT | Rolling 12mo sum |
| ttm_offer_acceptance_rate | FLOAT | accepted / extended |
| ttm_avg_time_to_fill | FLOAT | Rolling 12mo weighted avg |
| orgwide_ttm_offer_acceptance_rate | FLOAT | Company-wide benchmark |
| orgwide_ttm_avg_time_to_fill | FLOAT | Company-wide benchmark |

**Note:** Internal hires (origin = "internal") excluded from time-to-fill calculations.

### fct_workforce_composition

**Grain:** One row per (report_month x dimension combo from workforce grid)
**Row count estimate:** ~10,000-20,000

| Field | Type | Description |
|-------|------|-------------|
| report_month | DATE | From grid |
| report_quarter | STRING | |
| flag_end_of_quarter | BOOL | |
| flag_latest_report | BOOL | |
| department | STRING | Dimension |
| sub_department | STRING | Dimension |
| job_level | STRING | Dimension |
| level_group | STRING | Dimension |
| gender | STRING | Dimension |
| race_ethnicity | STRING | Dimension |
| manager_status | BOOL | Dimension |
| headcount | INT | Active employees |
| hires | INT | New hires this month |
| terminations | INT | All terminations |
| net_change | INT | hires - terminations |
| pct_of_total_headcount | FLOAT | headcount / orgwide_headcount |
| gender_representation_rate | FLOAT | headcount / department_headcount (for gender within dept) |
| race_representation_rate | FLOAT | Same for race |
| avg_span_of_control | FLOAT | AVG(no_direct_reports) where manager_status = TRUE |
| avg_tenure_months | FLOAT | AVG(tenure_months) |
| orgwide_headcount | INT | Company-wide headcount |
| orgwide_avg_span_of_control | FLOAT | Company-wide avg span |

### fct_compensation_reporting

**Grain:** One row per (report_month x dimension combo from comp grid)
**Row count estimate:** ~5,000-10,000

| Field | Type | Description |
|-------|------|-------------|
| report_month | DATE | From grid |
| report_quarter | STRING | |
| flag_end_of_quarter | BOOL | |
| flag_latest_report | BOOL | |
| department | STRING | Dimension |
| sub_department | STRING | Dimension |
| job_level | STRING | Dimension |
| level_group | STRING | Dimension |
| gender | STRING | Dimension |
| latest_performance_rating | STRING | Dimension |
| employee_count | INT | Active employees |
| avg_salary | FLOAT | AVG(salary) |
| avg_compa_ratio | FLOAT | AVG(compa_ratio) |
| avg_band_position | FLOAT | AVG((salary - band_min) / (band_max - band_min)) |
| count_below_band | INT | COUNT where compa_ratio < 0.90 |
| count_above_band | INT | COUNT where compa_ratio > 1.10 |
| median_compa_ratio | FLOAT | Median |
| orgwide_avg_compa_ratio | FLOAT | Company-wide avg |
| orgwide_avg_salary | FLOAT | Company-wide avg salary |
| dept_avg_compa_ratio | FLOAT | Department-level avg |

### fct_employee_roster

**Grain:** One row per employee per month (promoted from int_employee_monthly_roster)
**Row count estimate:** ~65,000-75,000
**Purpose:** Drill-through table for Tableau. When the CHRO asks "who left?" or "who is below band?", filter this table.

All fields from int_employee_monthly_roster, plus:

| Field | Type | Description |
|-------|------|-------------|
| band_position_label | STRING | "Below Band" if compa_ratio < 0.90, "Within Band" if 0.90-1.10, "Above Band" if > 1.10 |
| is_active | BOOL | employment_status = "Active" |

---

## Retained Models (from V0, no changes)

### Intermediate
- **int_employee_tenure** - tenure_months, total_promotions, career_velocity
- **int_employee_compensation_current** - Latest comp per employee
- **int_employee_performance_history** - Rating history, deltas, is_top_performer
- **int_recruiting_funnel_metrics** - Per-requisition funnel volumes
- **int_engagement_theme_rollup** - 27 questions collapsed to 8 themes

### Marts
- **fct_recruiting_velocity** - Per-requisition detail (drill-through for hiring view)
- **fct_engagement_trends** - Theme scores by department and cycle
- **fct_performance_distribution** - Per (employee, cycle) ratings
