"""
Stage 3 deliverable 5: raw_engagement (Lattice, anonymized).

Purpose
-------
For every (survey_cycle, department, question_id) combination, generate
a row carrying the aggregated avg_score, favorable_pct, response_count,
and the dept-cycle enps_score. Output is anonymized at the department
level; sub-department rows are only included when the cell's response
count would be >= 5 (Section 12 anonymity rule).

Inputs
------
- Stage 1 profiles (for active-headcount-by-dept-by-cycle calculation)

Outputs
-------
build_raw_engagement() returns a DataFrame with the 11 columns from
the data dictionary. No employee_id column (anonymized table).

Schema notes
------------
- avg_score is on the 1-5 Likert scale.
- enps_score is on the -100 to 100 NPS scale and is the same value
  across all rows that share (survey_cycle, department).
- The eNPS question itself is not emitted as a separate row; the
  data dictionary models it as a per-row dimension. This produces
  27 standard-question rows per (cycle, dept) cell, slightly below
  the spec's "28 questions" framing but consistent with the schema's
  enps_score field design.

Design choices
--------------
Score generation:
  base_avg = Section 10 baseline (per theme, company-wide 2025-Q1)
  + dept_variation (Section 10 dept deltas per theme)
  + time_trend (piecewise shift across the 16 cycles, capturing the
    growth optimism 2021-2022, pre-layoff drift 2022-Q3 onward,
    sharp post-layoff drop 2023-Q2/Q3, and gradual 2023-2025 recovery)
  + small per-cell noise (~+/- 0.05)

Favorable percentage:
  Linear from avg_score (rough industry rule of thumb: a 3.5 avg maps
  to ~55% favorable, a 4.0 avg to ~75%). The exact slope is calibrated
  so the company-wide baselines in Section 10 land at their stated
  favorable_pct values.

eNPS:
  Section 10 lists 2025-Q1 eNPS by department (Engineering 25,
  Sales 15, CS 35, Marketing 30, Product 40, G&A 20, People 28).
  Time trend applied as a 35x multiple on the engagement-score time
  shift, so the post-layoff dip is sharper on eNPS than on Likert
  scores -- realistic for an "I'd recommend this company" sentiment.

Response count:
  Active headcount in the dept at cycle_date * uniform(0.78, 0.88)
  per Section 10's response-rate range.

Sub-department rows:
  Disabled in this build to keep total volume close to the spec
  target. Adding sub-dept rows for the largest sub-departments
  (Engineering Platform / AI-ML / Data / Infrastructure, Sales AE)
  would roughly double the row count. If a downstream Tableau
  workbook needs sub-dept granularity, the script can be extended
  with the suppression-at-5 rule.
"""

from __future__ import annotations

import random
import runpy
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd

RANDOM_SEED = 20260425

# Survey cycles (quarterly, end-of-quarter date as cycle_date for
# headcount lookup; spec says surveys "run quarterly Q1 (Mar), Q2 (Jun),
# Q3 (Sep), Q4 (Dec)").
CYCLES: list[tuple[str, date]] = [
    ("2021-Q2", date(2021, 6, 30)),
    ("2021-Q3", date(2021, 9, 30)),
    ("2021-Q4", date(2021, 12, 31)),
    ("2022-Q1", date(2022, 3, 31)),
    ("2022-Q2", date(2022, 6, 30)),
    ("2022-Q3", date(2022, 9, 30)),
    ("2022-Q4", date(2022, 12, 31)),
    ("2023-Q1", date(2023, 3, 31)),
    ("2023-Q2", date(2023, 6, 30)),
    ("2023-Q3", date(2023, 9, 30)),
    ("2023-Q4", date(2023, 12, 31)),
    ("2024-Q1", date(2024, 3, 31)),
    ("2024-Q2", date(2024, 6, 30)),
    ("2024-Q3", date(2024, 9, 30)),
    ("2024-Q4", date(2024, 12, 31)),
    ("2025-Q1", date(2025, 3, 31)),
]

DEPARTMENTS: tuple[str, ...] = (
    "Engineering", "Sales", "Customer Success", "Marketing",
    "Product", "G&A", "People",
)

# Theme abbreviation -> full name.
THEME_FULL_NAMES: dict[str, str] = {
    "ENG": "Employee Engagement",
    "MGE": "Manager Effectiveness",
    "CGD": "Career Growth & Development",
    "WLB": "Work-Life Balance",
    "REC": "Recognition",
    "CUL": "Company Culture",
    "COM": "Communication",
    "RES": "Resources & Enablement",
}

# (question_id, theme_abbrev, question_text). Order matches Section 10.
QUESTIONS: list[tuple[str, str, str]] = [
    # ENG (4 questions)
    ("ENG1", "ENG", "I would recommend this company as a great place to work."),
    ("ENG2", "ENG", "I see myself still working here in two years."),
    ("ENG3", "ENG", "I am proud to tell others I work at this company."),
    ("ENG4", "ENG", "I feel motivated to go above and beyond what is expected of me."),
    # MGE (5 questions)
    ("MGE1", "MGE", "My manager gives me regular and meaningful feedback on my work."),
    ("MGE2", "MGE", "My manager supports my professional development and career growth."),
    ("MGE3", "MGE", "My manager keeps me informed about how my work connects to the broader picture."),
    ("MGE4", "MGE", "My manager creates an environment where I feel comfortable raising concerns."),
    ("MGE5", "MGE", "My manager treats all team members fairly and equitably."),
    # CGD (3 questions)
    ("CGD1", "CGD", "I have access to the learning and development opportunities I need to grow in my career."),
    ("CGD2", "CGD", "I can see a clear path for my career progression at this company."),
    ("CGD3", "CGD", "The work I do is meaningful and contributes to my professional growth."),
    # WLB (3 questions)
    ("WLB1", "WLB", "I am able to maintain a healthy balance between my work and personal life."),
    ("WLB2", "WLB", "My workload is manageable and sustainable."),
    ("WLB3", "WLB", "I feel supported when I need to take time off for personal reasons."),
    # REC (3 questions)
    ("REC1", "REC", "I receive adequate recognition when I do good work."),
    ("REC2", "REC", "Recognition at this company is given fairly and consistently."),
    ("REC3", "REC", "My contributions are valued by my team and leadership."),
    # CUL (3 questions)
    ("CUL1", "CUL", "I feel a strong sense of belonging at this company."),
    ("CUL2", "CUL", "The company's values are reflected in how we operate day-to-day."),
    ("CUL3", "CUL", "I trust the leadership team to make good decisions for the company."),
    # COM (3 questions)
    ("COM1", "COM", "I receive clear and timely communication about company strategy and direction."),
    ("COM2", "COM", "I understand how my work contributes to the company's goals."),
    ("COM3", "COM", "Cross-team communication and collaboration is effective."),
    # RES (3 questions)
    ("RES1", "RES", "I have access to the tools and technology I need to do my job effectively."),
    ("RES2", "RES", "I have the information and context I need to make good decisions in my role."),
    ("RES3", "RES", "Our team processes and workflows enable me to do my best work."),
]
assert len(QUESTIONS) == 27, f"Expected 27 questions, got {len(QUESTIONS)}"

# Section 10 baseline (company-wide, 2025-Q1) per theme.
BASELINE_AVG_BY_THEME: dict[str, float] = {
    "ENG": 3.7,
    "MGE": 3.9,
    "CGD": 3.4,
    "WLB": 3.5,
    "REC": 3.3,
    "CUL": 3.8,
    "COM": 3.5,
    "RES": 3.7,
}
BASELINE_FAV_BY_THEME: dict[str, float] = {
    "ENG": 0.68,
    "MGE": 0.74,
    "CGD": 0.55,
    "WLB": 0.58,
    "REC": 0.50,
    "CUL": 0.72,
    "COM": 0.56,
    "RES": 0.66,
}

# Section 10 dept deltas. Theme-level shifts (positive = better than
# company average; negative = worse).
DEPT_THEME_VARIATIONS: dict[str, dict[str, float]] = {
    "Engineering":      {"RES": +0.30, "CUL": +0.20, "WLB": -0.30, "REC": -0.20},
    "Sales":            {"ENG": +0.10, "WLB": -0.40, "CGD": -0.30, "REC": -0.30},
    "Customer Success": {"CUL": +0.30, "MGE": +0.20, "COM": -0.20},
    "Marketing":        {"CUL": +0.20, "REC": +0.20, "RES": -0.20},
    "Product":          {"CGD": +0.30, "RES": +0.30, "COM": -0.20},
    "G&A":              {"WLB": +0.20, "CGD": -0.20, "ENG": -0.10},
    "People":           {"CUL": +0.30, "MGE": +0.20, "WLB": -0.20, "RES": -0.20},
}

# Section 10 eNPS by department for 2025-Q1.
DEPT_ENPS_2025_Q1: dict[str, float] = {
    "Engineering":      25.0,
    "Sales":            15.0,
    "Customer Success": 35.0,
    "Marketing":        30.0,
    "Product":          40.0,
    "G&A":              20.0,
    "People":           28.0,
}

# Time trend per cycle (Section 10 narrative). Applied to avg_score
# (Likert 1-5) and scaled up for eNPS. 2025-Q1 is the baseline (0).
SCORE_TIME_SHIFT: dict[str, float] = {
    "2021-Q2": +0.00,
    "2021-Q3": +0.05,
    "2021-Q4": +0.10,
    "2022-Q1": +0.15,
    "2022-Q2": +0.20,   # peak (growth optimism)
    "2022-Q3": +0.10,
    "2022-Q4": -0.05,   # pre-layoff drift
    "2023-Q1": -0.20,   # layoff hits
    "2023-Q2": -0.35,   # post-layoff trauma trough
    "2023-Q3": -0.30,
    "2023-Q4": -0.20,
    "2024-Q1": -0.15,
    "2024-Q2": -0.10,
    "2024-Q3": -0.05,
    "2024-Q4": -0.02,
    "2025-Q1": +0.00,   # baseline
}

ENPS_TIME_MULTIPLIER = 35.0
RESPONSE_RATE_RANGE = (0.78, 0.88)
SCORE_NOISE_RANGE = (-0.05, 0.05)
ENPS_NOISE_RANGE = (-2.0, 2.0)
FAV_PCT_NOISE_RANGE = (-0.02, 0.02)

ENGAGEMENT_COLUMNS = [
    "survey_cycle",
    "department",
    "sub_department",
    "question_id",
    "question_text",
    "theme",
    "avg_score",
    "response_count",
    "favorable_pct",
    "enps_score",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_date(value) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _load_profiles() -> pd.DataFrame:
    base = Path(__file__).parent
    stage1 = runpy.run_path(str(base / "01_generate_employee_profiles.py"), run_name="stage1")
    return stage1["build_employee_profiles"]()


def _active_headcount_by_dept(profiles: pd.DataFrame, cycle_date: date) -> dict[str, int]:
    """Count active employees per department on cycle_date."""
    counts: dict[str, int] = defaultdict(int)
    for _, profile in profiles.iterrows():
        hire_d = _to_date(profile["hire_date"])
        if hire_d > cycle_date:
            continue
        term_d = profile["termination_date"]
        if term_d is not None and not pd.isna(term_d):
            term_d = _to_date(term_d)
            if term_d < cycle_date:
                continue
        if profile["department"] in DEPARTMENTS:
            counts[profile["department"]] += 1
    return counts


def _compute_avg_score(
    rng: random.Random, theme: str, department: str, cycle_id: str
) -> float:
    base = BASELINE_AVG_BY_THEME[theme]
    dept_var = DEPT_THEME_VARIATIONS.get(department, {}).get(theme, 0.0)
    time_var = SCORE_TIME_SHIFT.get(cycle_id, 0.0)
    noise = rng.uniform(*SCORE_NOISE_RANGE)
    score = base + dept_var + time_var + noise
    return max(1.0, min(5.0, score))


def _avg_to_favorable(avg_score: float, theme: str, rng: random.Random) -> float:
    """Map avg_score to favorable_pct.

    Calibrated so the Section 10 baselines (e.g. ENG avg 3.7 / fav 0.68,
    MGE avg 3.9 / fav 0.74, REC avg 3.3 / fav 0.50) land near their
    stated values. Linear in avg_score with a small theme-specific
    intercept and per-cell noise.
    """
    # Theme-specific base offset to anchor at 2025-Q1 baseline.
    expected_base_fav = BASELINE_FAV_BY_THEME[theme]
    expected_base_avg = BASELINE_AVG_BY_THEME[theme]
    # Slope of 0.45 per Likert point gives the Section 10 baselines a tight match.
    fav = expected_base_fav + (avg_score - expected_base_avg) * 0.45
    fav += rng.uniform(*FAV_PCT_NOISE_RANGE)
    return max(0.0, min(1.0, fav))


def _compute_enps(
    rng: random.Random, department: str, cycle_id: str
) -> float:
    base = DEPT_ENPS_2025_Q1[department]
    time_shift = SCORE_TIME_SHIFT.get(cycle_id, 0.0) * ENPS_TIME_MULTIPLIER
    noise = rng.uniform(*ENPS_NOISE_RANGE)
    enps = base + time_shift + noise
    return max(-100.0, min(100.0, enps))


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build_raw_engagement() -> pd.DataFrame:
    """Build raw_engagement at the (cycle, department, question_id) granularity."""
    profiles = _load_profiles()
    rng = random.Random(RANDOM_SEED)

    rows: list[dict] = []
    for cycle_id, cycle_date in CYCLES:
        headcount_by_dept = _active_headcount_by_dept(profiles, cycle_date)
        for department in DEPARTMENTS:
            active_count = headcount_by_dept.get(department, 0)
            # Pick the integer response_count that yields a rate in
            # [0.78, 0.88]. For very small departments where no integer
            # produces a rate in range, fall back to the nearest integer
            # to 0.83 * active_count.
            min_count = -(-(active_count * 78) // 100)  # ceil(0.78 * n)
            max_count = (active_count * 88) // 100      # floor(0.88 * n)
            if min_count > max_count or active_count == 0:
                response_count = round(active_count * 0.83) if active_count > 0 else 0
            else:
                response_count = rng.randint(int(min_count), int(max_count))
            enps_score = _compute_enps(rng, department, cycle_id)

            for question_id, theme, question_text in QUESTIONS:
                avg_score = _compute_avg_score(rng, theme, department, cycle_id)
                fav_pct = _avg_to_favorable(avg_score, theme, rng)

                rows.append({
                    "survey_cycle":   cycle_id,
                    "department":     department,
                    "sub_department": None,
                    "question_id":    question_id,
                    "question_text":  question_text,
                    "theme":          THEME_FULL_NAMES[theme],
                    "avg_score":      round(avg_score, 1),
                    "response_count": response_count,
                    "favorable_pct":  round(fav_pct, 2),
                    "enps_score":     round(enps_score, 1),
                })

    df = pd.DataFrame(rows, columns=ENGAGEMENT_COLUMNS)
    df = df.sort_values(["survey_cycle", "department", "question_id"]).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame) -> None:
    print(f"\n=== raw_engagement ===")
    print(f"  total rows:       {len(df)}")
    print(f"  unique cycles:    {df['survey_cycle'].nunique()}")
    print(f"  unique depts:     {df['department'].nunique()}")
    print(f"  unique questions: {df['question_id'].nunique()}")

    print("\n  Rows per cycle (one per dept x question):")
    print(df["survey_cycle"].value_counts().sort_index().to_string())

    print("\n  2025-Q1 average avg_score by department (vs Section 10 dept variations):")
    latest = df[df["survey_cycle"] == "2025-Q1"]
    by_dept = latest.groupby("department")["avg_score"].mean().round(2)
    print(by_dept.to_string())

    print("\n  2025-Q1 avg_score by theme x department (Likert 1-5):")
    pivot = latest.pivot_table(
        index="department", columns="theme", values="avg_score", aggfunc="mean"
    ).round(2)
    print(pivot.to_string())

    print("\n  2025-Q1 favorable_pct by theme (company-wide vs Section 10 baseline):")
    fav_by_theme = latest.groupby("theme")["favorable_pct"].mean().round(3) * 100
    print(fav_by_theme.to_string())
    print("\n  Section 10 baselines:")
    for theme_short, fav in BASELINE_FAV_BY_THEME.items():
        print(f"    {THEME_FULL_NAMES[theme_short]}: {fav*100:.0f}%")

    print("\n  eNPS by department, 2025-Q1 (vs Section 10 target):")
    enps = latest.groupby("department")["enps_score"].first().round(1)
    print(enps.to_string())
    print("\n  Section 10 targets:")
    for dept, target in DEPT_ENPS_2025_Q1.items():
        print(f"    {dept}: {target}")

    print("\n  Time trend check: Engineering ENG1 avg_score across cycles")
    eng_eng1 = (
        df[(df["department"] == "Engineering") & (df["question_id"] == "ENG1")]
        .sort_values("survey_cycle")
        [["survey_cycle", "avg_score", "favorable_pct", "enps_score", "response_count"]]
    )
    print(eng_eng1.to_string(index=False))

    print("\n  Response count by cycle (Engineering):")
    eng_response = (
        df[(df["department"] == "Engineering") & (df["question_id"] == "ENG1")]
        .sort_values("survey_cycle")
        [["survey_cycle", "response_count"]]
    )
    print(eng_response.to_string(index=False))

    print("\n  Coherence checks:")
    print(f"    avg_score in [1, 5]:  {((df['avg_score'] >= 1) & (df['avg_score'] <= 5)).all()}")
    print(f"    fav_pct in [0, 1]:    {((df['favorable_pct'] >= 0) & (df['favorable_pct'] <= 1)).all()}")
    print(f"    enps in [-100, 100]:  {((df['enps_score'] >= -100) & (df['enps_score'] <= 100)).all()}")
    print(f"    employee_id absent:   {'employee_id' not in df.columns}")
    print(f"    response_count > 0:   {(df['response_count'] > 0).all()}")


if __name__ == "__main__":
    df = build_raw_engagement()
    print_summary(df)
