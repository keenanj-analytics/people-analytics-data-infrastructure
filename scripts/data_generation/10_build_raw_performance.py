"""
Stage 3 deliverable 3: raw_performance (Lattice).

Purpose
-------
Generate one row per (employee, eligible review cycle). Reviews follow
Section 7 archetype rating probabilities, with three structural overrides:

    Promotion coherence  -- Section 12: the cycle ending immediately
                            before each Promotion in raw_job_history
                            must show 'Exceeds' or 'Significantly
                            Exceeds'. Lower draws are upgraded to
                            'Exceeds' so promotions remain spec-valid.
    Performance managed  -- Section 7: this archetype's distribution
                            differs early vs late. The last two
                            eligible cycles of each Performance
                            Managed Out profile use the late
                            distribution (declining); earlier cycles
                            use the early distribution.
    Manager Step-Back    -- Section 7: distribution differs while at
                            M1 vs after returning to IC. Cycles whose
                            cycle_end falls before the step-back date
                            (the M1 -> IC4 Title Change event in
                            raw_job_history) use the 'as M1'
                            distribution; later cycles use the 'after
                            IC return' distribution.

Inputs
------
- Stage 1 profiles
- Stage 2e raw_employees (for current job_level -> Exempt status check)
- Stage 8 raw_job_history (Promotion + Title Change dates)

Outputs
-------
build_raw_performance() returns a DataFrame keyed by (employee_id,
review_cycle) with the seven Lattice columns from the data dictionary.

Cycle schedule (per Section 7)
------------------------------
H1 reviews complete July 15; H2 reviews complete January 15 of the
next year. First cycle: 2020-H2 (completed 2021-01-15). Last cycle in
this dataset: 2024-H2 (completed 2025-01-15).

Eligibility
-----------
- hire_date + 90 days <= cycle_end (3-month minimum tenure)
- termination_date >= cycle_end (no reviews after termination)

Other fields
------------
- manager_rating: equals overall_rating 90% of the time; 10% drift one
  level up or down (post-calibration cleanup vs manager input).
- self_rating: equals overall_rating 70% of the time; 30% chance one
  level higher (employee self-assessment optimism).
- review_completed_date: cycle_end +/- 5 days.
- review_status: Exempt for M5 leadership reviews (per Section 7
  "1% Exempt (executives)"); 5% Incomplete for everyone else; rest
  Completed.
"""

from __future__ import annotations

import random
import runpy
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

RANDOM_SEED = 20260425
CURRENT_DATE = date(2025, 3, 31)

CYCLES: list[tuple[str, date]] = [
    ("2020-H2", date(2021, 1, 15)),
    ("2021-H1", date(2021, 7, 15)),
    ("2021-H2", date(2022, 1, 15)),
    ("2022-H1", date(2022, 7, 15)),
    ("2022-H2", date(2023, 1, 15)),
    ("2023-H1", date(2023, 7, 15)),
    ("2023-H2", date(2024, 1, 15)),
    ("2024-H1", date(2024, 7, 15)),
    ("2024-H2", date(2025, 1, 15)),
]

RATINGS = [
    "Significantly Exceeds",
    "Exceeds",
    "Meets",
    "Partially Meets",
    "Does Not Meet",
]
RATING_INDEX = {rating: i for i, rating in enumerate(RATINGS)}

# Section 7 distributions. Order matches RATINGS.
ARCHETYPE_RATING_DIST: dict[str, list[float]] = {
    "High-flyer":                [0.30, 0.55, 0.15, 0.00, 0.00],
    "Steady contributor":        [0.02, 0.23, 0.65, 0.08, 0.02],
    "Early churner":             [0.00, 0.15, 0.55, 0.25, 0.05],
    "Top performer flight risk": [0.25, 0.60, 0.15, 0.00, 0.00],
    "Layoff casualty":           [0.05, 0.20, 0.45, 0.20, 0.10],
    "Internal mover":            [0.05, 0.35, 0.55, 0.05, 0.00],
    "Manager change casualty":   [0.05, 0.35, 0.55, 0.05, 0.00],
    "Founder / early employee":  [0.35, 0.55, 0.10, 0.00, 0.00],
    # Defined leadership not in Section 7 explicitly; map to High-flyer-
    # like distribution (top performers; the M3-M5 leaders are senior
    # operators with consistent 'Exceeds' performance).
    "Defined leadership":        [0.30, 0.55, 0.15, 0.00, 0.00],
}

PERF_MANAGED_EARLY: list[float] = [0.00, 0.10, 0.60, 0.25, 0.05]
PERF_MANAGED_LATE:  list[float] = [0.00, 0.00, 0.15, 0.50, 0.35]

STEP_BACK_AS_M1:        list[float] = [0.00, 0.15, 0.60, 0.25, 0.00]
STEP_BACK_AFTER_RETURN: list[float] = [0.05, 0.40, 0.50, 0.05, 0.00]

INCOMPLETE_RATE = 0.05
MANAGER_RATING_DRIFT_RATE = 0.10
SELF_RATING_OPTIMISM_RATE = 0.30
EXEMPT_LEVELS: frozenset[str] = frozenset({"M5"})

PERFORMANCE_COLUMNS = [
    "employee_id",
    "review_cycle",
    "overall_rating",
    "manager_rating",
    "self_rating",
    "review_completed_date",
    "review_status",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_date(value) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _load_state():
    """Run upstream stages and return profiles, raw_employees, job_history."""
    base = Path(__file__).parent
    stage1 = runpy.run_path(str(base / "01_generate_employee_profiles.py"), run_name="stage1")
    stage2e = runpy.run_path(str(base / "07_materialize_raw_employees.py"), run_name="stage2e")
    stage3a = runpy.run_path(str(base / "08_complete_raw_job_history.py"), run_name="stage3a")
    return (
        stage1["build_employee_profiles"](),
        stage2e["build_raw_employees"](),
        stage3a["build_raw_job_history"](),
    )


def _eligible_cycles(hire_d: date, end_d: date) -> list[tuple[str, date]]:
    """Return cycles where hire_d + 90 days <= cycle_end <= end_d."""
    return [
        (cycle_id, cycle_end)
        for cycle_id, cycle_end in CYCLES
        if (cycle_end - hire_d).days >= 90 and cycle_end <= end_d
    ]


def _sample_rating(rng: random.Random, dist: list[float]) -> str:
    return rng.choices(RATINGS, weights=dist, k=1)[0]


def _bump_to_at_least_exceeds(rating: str) -> str:
    """For Promotion coherence: ensure prior cycle is Exceeds or higher."""
    if RATING_INDEX[rating] > RATING_INDEX["Exceeds"]:
        return "Exceeds"
    return rating


def _shift_rating(rating: str, shift: int) -> str:
    """Move rating up (negative shift) or down (positive shift); clip at ends."""
    new_idx = max(0, min(len(RATINGS) - 1, RATING_INDEX[rating] + shift))
    return RATINGS[new_idx]


def _find_prior_cycle(promo_date: date) -> str | None:
    """Cycle ending immediately before promo_date."""
    candidates = [(cid, end) for cid, end in CYCLES if end < promo_date]
    if not candidates:
        return None
    return max(candidates, key=lambda item: item[1])[0]


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build_raw_performance() -> pd.DataFrame:
    """Build raw_performance for all eligible (employee, cycle) pairs."""
    profiles, raw_employees, job_history = _load_state()
    rng = random.Random(RANDOM_SEED)

    promo_dates_by_emp: dict[str, list[date]] = defaultdict(list)
    step_back_date_by_emp: dict[str, date] = {}
    for _, row in job_history.iterrows():
        emp_id = row["employee_id"]
        change_type = row["change_type"]
        if change_type == "Promotion":
            promo_dates_by_emp[emp_id].append(_to_date(row["effective_date"]))
        elif change_type == "Title Change":
            step_back_date_by_emp[emp_id] = _to_date(row["effective_date"])

    current_level_by_emp = raw_employees.set_index("employee_id")["job_level"].to_dict()

    # First pass: sample overall ratings per cycle per employee with the
    # archetype-and-state-aware distribution.
    reviews_by_emp: dict[str, dict[str, dict]] = defaultdict(dict)

    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        archetype = profile["archetype"]
        hire_d = _to_date(profile["hire_date"])
        end_d = (
            _to_date(profile["termination_date"])
            if profile["employment_status"] == "Terminated"
            else CURRENT_DATE
        )
        eligible = _eligible_cycles(hire_d, end_d)
        if not eligible:
            continue

        is_perf_managed = archetype == "Performance managed out"
        late_cycle_ids: set[str] = set()
        if is_perf_managed and eligible:
            # The trailing two cycles use the late (declining) distribution.
            late_cycle_ids = {cid for cid, _end in eligible[-2:]}

        is_step_back = archetype == "Manager step-back"
        step_back_d = step_back_date_by_emp.get(emp_id)

        for cycle_id, cycle_end in eligible:
            if is_perf_managed:
                dist = PERF_MANAGED_LATE if cycle_id in late_cycle_ids else PERF_MANAGED_EARLY
            elif is_step_back and step_back_d is not None:
                dist = STEP_BACK_AFTER_RETURN if cycle_end > step_back_d else STEP_BACK_AS_M1
            else:
                dist = ARCHETYPE_RATING_DIST.get(
                    archetype, ARCHETYPE_RATING_DIST["Steady contributor"]
                )
            overall = _sample_rating(rng, dist)
            reviews_by_emp[emp_id][cycle_id] = {
                "overall_rating": overall,
                "cycle_end": cycle_end,
            }

    # Second pass: Promotion coherence. The cycle ending immediately
    # before each Promotion must be Exceeds or higher.
    for emp_id, promo_dates in promo_dates_by_emp.items():
        emp_reviews = reviews_by_emp.get(emp_id, {})
        for promo_date in promo_dates:
            prior_cycle = _find_prior_cycle(promo_date)
            if prior_cycle and prior_cycle in emp_reviews:
                emp_reviews[prior_cycle]["overall_rating"] = _bump_to_at_least_exceeds(
                    emp_reviews[prior_cycle]["overall_rating"]
                )

    # Third pass: emit rows with manager_rating, self_rating, status,
    # review_completed_date.
    profile_dates_by_emp: dict[str, tuple[date, date]] = {}
    for _, profile in profiles.iterrows():
        hire_d = _to_date(profile["hire_date"])
        end_d = (
            _to_date(profile["termination_date"])
            if profile["employment_status"] == "Terminated"
            else CURRENT_DATE
        )
        profile_dates_by_emp[profile["employee_id"]] = (hire_d, end_d)

    rows: list[dict] = []
    for emp_id, emp_reviews in reviews_by_emp.items():
        current_level = current_level_by_emp.get(emp_id, "IC1")
        hire_d, end_d = profile_dates_by_emp[emp_id]
        completed_min = hire_d + timedelta(days=90)
        completed_max = end_d
        for cycle_id, info in emp_reviews.items():
            overall = info["overall_rating"]
            cycle_end = info["cycle_end"]

            # Manager rating: 90% match overall; 10% drift +/- 1.
            if rng.random() < MANAGER_RATING_DRIFT_RATE:
                manager_rating = _shift_rating(overall, rng.choice([-1, 1]))
            else:
                manager_rating = overall

            # Self rating: 70% match overall; 30% one level higher (optimism).
            if (
                rng.random() < SELF_RATING_OPTIMISM_RATE
                and RATING_INDEX[overall] > 0
            ):
                self_rating = _shift_rating(overall, -1)
            else:
                self_rating = overall

            if current_level in EXEMPT_LEVELS:
                status = "Exempt"
            elif rng.random() < INCOMPLETE_RATE:
                status = "Incomplete"
            else:
                status = "Completed"

            completed_date = cycle_end + timedelta(days=rng.randint(-5, 5))
            # Clamp to the profile's eligibility window so jitter never
            # pushes a review before hire + 90 days or after termination.
            if completed_date < completed_min:
                completed_date = completed_min
            if completed_date > completed_max:
                completed_date = completed_max

            rows.append({
                "employee_id":           emp_id,
                "review_cycle":          cycle_id,
                "overall_rating":        overall,
                "manager_rating":        manager_rating,
                "self_rating":           self_rating,
                "review_completed_date": completed_date,
                "review_status":         status,
            })

    df = pd.DataFrame(rows, columns=PERFORMANCE_COLUMNS)
    df = df.sort_values(["employee_id", "review_cycle"]).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame) -> None:
    print(f"\n=== raw_performance ===")
    print(f"  total rows:       {len(df)}")
    print(f"  unique employees: {df['employee_id'].nunique()}")

    print("\n  Rows per cycle:")
    print(df["review_cycle"].value_counts().sort_index().to_string())

    print("\n  Reviews per employee summary:")
    print(df.groupby("employee_id").size().describe().round(1).to_string())

    print("\n  overall_rating distribution:")
    print(df["overall_rating"].value_counts(normalize=True).round(3).mul(100).to_string())

    print("\n  review_status distribution:")
    print(df["review_status"].value_counts(normalize=True).round(3).mul(100).to_string())

    print("\n  overall_rating by archetype (% of archetype's reviews):")
    profiles, _, _ = _load_state()
    merged = df.merge(profiles[["employee_id", "archetype"]], on="employee_id")
    by_arch = pd.crosstab(
        merged["archetype"], merged["overall_rating"], normalize="index"
    ).round(3) * 100
    for col in RATINGS:
        if col not in by_arch.columns:
            by_arch[col] = 0.0
    print(by_arch[RATINGS].to_string())

    print("\n  Coherence check: Promotion prior cycle is Exceeds or higher")
    _validate_promotion_coherence(df)

    print("\n  Coherence check: no reviews after termination_date")
    terminated = profiles[profiles["employment_status"] == "Terminated"][
        ["employee_id", "termination_date"]
    ].copy()
    terminated["termination_date"] = pd.to_datetime(terminated["termination_date"])
    after_term = df.merge(terminated, on="employee_id")
    after_term = after_term[
        after_term["review_completed_date"] > after_term["termination_date"]
    ]
    print(f"    rows after termination: {len(after_term)}  (expected 0 minus completion-jitter slop)")

    print("\n  Coherence check: no reviews before hire + 3 months")
    hire_check = df.merge(profiles[["employee_id", "hire_date"]], on="employee_id")
    hire_check["hire_date"] = pd.to_datetime(hire_check["hire_date"])
    hire_check["min_eligible"] = hire_check["hire_date"] + pd.Timedelta(days=90)
    too_early = hire_check[hire_check["review_completed_date"] < hire_check["min_eligible"]]
    print(f"    rows before hire+90d: {len(too_early)}  (expected 0 minus jitter slop)")


def _validate_promotion_coherence(df: pd.DataFrame) -> None:
    _, _, job_history = _load_state()
    promotion_rows = job_history[job_history["change_type"] == "Promotion"]

    review_lookup = df.set_index(["employee_id", "review_cycle"])[
        "overall_rating"
    ].to_dict()

    violations = []
    for _, promo in promotion_rows.iterrows():
        emp_id = promo["employee_id"]
        promo_d = _to_date(promo["effective_date"])
        prior_cycle = _find_prior_cycle(promo_d)
        if prior_cycle is None:
            continue
        rating = review_lookup.get((emp_id, prior_cycle))
        if rating is None:
            continue
        if RATING_INDEX[rating] > RATING_INDEX["Exceeds"]:
            violations.append((emp_id, promo_d, prior_cycle, rating))
    print(f"    promotions with prior cycle below Exceeds: {len(violations)}")
    if violations:
        for v in violations[:5]:
            print(f"      {v}")


if __name__ == "__main__":
    df = build_raw_performance()
    print_summary(df)
