"""
Stage 3 deliverable 2: raw_compensation (Pave).

Purpose
-------
Generate one row per compensation event per employee. Events:

    New Hire           -- every employee, effective_date = hire_date
    Promotion          -- one per Promotion in raw_job_history; bump 10-15%
                          and land within the new level's band
    Annual Review      -- Jan 15 each year while the employee has been
                          in seat at least 6 months; merit % per
                          archetype's typical performance pattern
    Market Adjustment  -- effective 2023-09-01 for every employee active
                          on that date; 5-8% bump applied company-wide
                          post Series B

Inputs
------
- Stage 1 profiles  (archetype + hire/term dates)
- Stage 2a designations (effective_starting_level for managers / founder IC)
- Stage 2c assignments (final dept / sub_dept / level)
- Stage 8 raw_job_history (for Promotion event dates and the at-hire
  state)

Outputs
-------
build_raw_compensation() returns a DataFrame keyed by (employee_id,
effective_date) with the seven columns from the data dictionary:

    employee_id, salary, comp_band_min, comp_band_mid, comp_band_max,
    effective_date, change_reason

Bands
-----
Sourced from the Ref - Job Architecture tab of the data dictionary.
Roughly 90 (department, sub_department, level) cells; cells outside
that table use a fallback (the same-department "All" wildcard, or
extrapolated from the nearest level). Each year between 2020 and 2025
gets a multiplier applied to the 2025 band so historical comp records
reflect the spec's progression: 2020 = 0.85x, 2021 = 0.88x,
2022 = 0.92x, 2023 = 0.95x, 2024 = 0.98x, 2025 = 1.00x.

Compa-ratio at hire is drawn from the archetype's range (Section 5).
Subsequent comp records walk the salary forward by the appropriate
bump; band Min / Mid / Max for the row reflects the year and the
employee's level at that moment.

Step-back caveat
----------------
The Manager Step-Back archetype's M1 -> IC4 demotion does NOT generate
a Pave row -- spec rule "Retained IC4 band after step-back (no pay
cut)". The next Annual Review will switch the band lookup to IC4
implicitly, but the salary continues unchanged through the step-back
moment.

Row count
---------
The data dictionary forecasts ~900-1,100 rows for raw_compensation,
but generating Annual Review records for every eligible Jan 15 across
604 profiles yields ~1,500-1,800 rows. The forecast appears to assume
a sparser model (Hire + Promotion + Market only). Annual Reviews are
included here per Section 6 ("Annual merit reviews happen in January
each year"), with the resulting count documented in the substage
review.
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
MARKET_ADJUSTMENT_DATE = date(2023, 9, 1)

LEVEL_ORDER = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "M3", "M4", "M5"]
LEVEL_INDEX = {level: i for i, level in enumerate(LEVEL_ORDER)}

COMPENSATION_COLUMNS = [
    "employee_id",
    "salary",
    "comp_band_min",
    "comp_band_mid",
    "comp_band_max",
    "effective_date",
    "change_reason",
]


# ---------------------------------------------------------------------------
# Compensation bands (Ref - Job Architecture tab, 2025 rates)
# ---------------------------------------------------------------------------

# Format: (department, sub_dept_or_All_wildcard, level) -> (min, mid, max).
# "All" matches any sub-department in that department for that level
# when no specific entry exists.
COMP_BANDS_2025: dict[tuple[str, str, str], tuple[int, int, int]] = {
    # Engineering (Technical)
    ("Engineering", "All", "IC1"):              (78000, 90000, 102000),
    ("Engineering", "All", "IC2"):              (102000, 117000, 132000),
    ("Engineering", "Platform", "IC3"):         (132000, 153000, 174000),
    ("Engineering", "Infrastructure", "IC3"):   (132000, 153000, 174000),
    ("Engineering", "AI/ML", "IC3"):            (138000, 160000, 182000),
    ("Engineering", "Data", "IC3"):             (132000, 153000, 174000),
    ("Engineering", "Platform", "IC4"):         (174000, 198000, 222000),
    ("Engineering", "Infrastructure", "IC4"):   (174000, 198000, 222000),
    ("Engineering", "AI/ML", "IC4"):            (181000, 206000, 232000),
    ("Engineering", "Data", "IC4"):             (174000, 198000, 222000),
    ("Engineering", "Platform", "IC5"):         (222000, 243000, 264000),
    ("Engineering", "Infrastructure", "IC5"):   (222000, 243000, 264000),
    ("Engineering", "AI/ML", "IC5"):            (231000, 253000, 275000),
    ("Engineering", "Data", "IC5"):             (222000, 243000, 264000),
    ("Engineering", "All", "M1"):               (156000, 177000, 198000),
    ("Engineering", "All", "M2"):               (198000, 219000, 240000),
    ("Engineering", "All", "M3"):               (240000, 270000, 300000),
    ("Engineering", "All", "M4"):               (300000, 342000, 384000),
    ("Engineering", "(CTO)", "M5"):             (384000, 456000, 528000),
    # Product / Design (Technical-Adjacent)
    ("Product", "All", "IC1"):                  (71500, 82500, 93500),
    ("Product", "Product Management", "IC2"):   (93500, 107250, 121000),
    ("Product", "Design", "IC2"):               (93500, 107250, 121000),
    ("Product", "UX Research", "IC2"):          (93500, 107250, 121000),
    ("Product", "Product Management", "IC3"):   (121000, 140250, 159500),
    ("Product", "Design", "IC3"):               (121000, 140250, 159500),
    ("Product", "UX Research", "IC3"):          (121000, 140250, 159500),
    ("Product", "Product Management", "IC4"):   (159500, 181500, 203500),
    ("Product", "Design", "IC4"):               (159500, 181500, 203500),
    ("Product", "UX Research", "IC4"):          (159500, 181500, 203500),
    ("Product", "Design", "IC5"):               (203500, 230000, 256500),
    ("Product", "All", "M1"):                   (143000, 162250, 181500),
    ("Product", "All", "M2"):                   (181500, 200750, 220000),
    ("Product", "All", "M3"):                   (220000, 247500, 275000),
    ("Product", "(CPO)", "M5"):                 (352000, 418000, 484000),
    # Sales (Commercial)
    ("Sales", "SDR", "IC1"):                    (55000, 62500, 70000),
    ("Sales", "SDR", "IC2"):                    (65000, 75000, 85000),
    ("Sales", "Account Executive", "IC1"):      (75000, 87500, 100000),
    ("Sales", "Account Executive", "IC2"):      (85000, 97500, 110000),
    ("Sales", "Account Executive", "IC3"):      (110000, 127500, 145000),
    ("Sales", "Account Executive", "IC4"):      (140000, 160000, 180000),
    ("Sales", "Account Management", "IC1"):     (70000, 82000, 94000),
    ("Sales", "Account Management", "IC2"):     (80000, 92000, 104000),
    ("Sales", "Account Management", "IC3"):     (104000, 120000, 136000),
    ("Sales", "Account Management", "IC4"):     (136000, 155000, 174000),
    ("Sales", "Sales Engineering", "IC1"):      (85000, 97000, 109000),
    ("Sales", "Sales Engineering", "IC2"):      (95000, 109000, 123000),
    ("Sales", "Sales Engineering", "IC3"):      (123000, 142500, 162000),
    ("Sales", "Sales Engineering", "IC4"):      (162000, 184500, 207000),
    ("Sales", "SDR", "M1"):                     (100000, 115000, 130000),
    ("Sales", "Account Executive", "M1"):       (130000, 147500, 165000),
    ("Sales", "Account Management", "M1"):      (130000, 147500, 165000),
    ("Sales", "Sales Engineering", "M1"):       (130000, 147500, 165000),
    ("Sales", "All", "M2"):                     (165000, 182500, 200000),
    ("Sales", "All", "M3"):                     (200000, 225000, 250000),
    ("Sales", "All", "M4"):                     (250000, 285000, 320000),
    ("Sales", "(CRO)", "M5"):                   (320000, 380000, 440000),
    # Customer Success (Commercial-Adjacent)
    ("Customer Success", "Support", "IC1"):     (55000, 63000, 71000),
    ("Customer Success", "Support", "IC2"):     (72000, 83000, 94000),
    ("Customer Success", "Support", "IC3"):     (94000, 108000, 122000),
    ("Customer Success", "Support", "IC4"):     (122000, 140000, 158000),
    ("Customer Success", "CSM", "IC1"):         (70000, 80000, 90000),
    ("Customer Success", "CSM", "IC2"):         (81000, 93000, 105000),
    ("Customer Success", "CSM", "IC3"):         (105000, 121000, 138000),
    ("Customer Success", "CSM", "IC4"):         (138000, 157000, 176000),
    ("Customer Success", "Implementation", "IC1"): (75000, 87000, 99000),
    ("Customer Success", "Implementation", "IC2"): (85000, 97500, 110000),
    ("Customer Success", "Implementation", "IC3"): (110000, 127500, 145000),
    ("Customer Success", "Implementation", "IC4"): (140000, 160000, 178000),
    ("Customer Success", "Implementation", "IC5"): (178000, 200000, 222000),
    ("Customer Success", "All", "M1"):          (117000, 133000, 149000),
    ("Customer Success", "All", "M2"):          (149000, 165000, 181000),
    ("Customer Success", "(VP CS)", "M4"):      (238000, 271000, 304000),
    # Marketing
    ("Marketing", "All", "IC1"):                (58500, 67500, 76500),
    ("Marketing", "Growth", "IC2"):             (85000, 97500, 110000),
    ("Marketing", "Content", "IC2"):            (81000, 93000, 105000),
    ("Marketing", "Product Marketing", "IC2"):  (85000, 97500, 110000),
    ("Marketing", "Brand", "IC2"):              (81000, 93000, 105000),
    ("Marketing", "Growth", "IC3"):             (110000, 127500, 145000),
    ("Marketing", "Content", "IC3"):            (105000, 121000, 138000),
    ("Marketing", "Product Marketing", "IC3"):  (110000, 127500, 145000),
    ("Marketing", "Brand", "IC3"):              (105000, 121000, 138000),
    ("Marketing", "Growth", "IC4"):             (140000, 160000, 178000),
    ("Marketing", "Content", "IC4"):            (133000, 152000, 170000),
    ("Marketing", "Product Marketing", "IC4"):  (140000, 160000, 178000),
    ("Marketing", "Brand", "IC4"):              (133000, 152000, 170000),
    ("Marketing", "All", "M1"):                 (120000, 136000, 152000),
    ("Marketing", "All", "M2"):                 (152000, 168000, 185000),
    ("Marketing", "(VP Mktg)", "M4"):           (238000, 271000, 304000),
    # G&A (Operations)
    ("G&A", "Finance", "IC1"):                  (60000, 69000, 78000),
    ("G&A", "Finance", "IC2"):                  (78000, 90000, 101000),
    ("G&A", "Finance", "IC3"):                  (101000, 117000, 133000),
    ("G&A", "Finance", "IC4"):                  (133000, 152000, 170000),
    ("G&A", "Legal", "IC1"):                    (66000, 75000, 84000),
    ("G&A", "Legal", "IC2"):                    (76500, 88000, 99000),
    ("G&A", "Legal", "IC3"):                    (120000, 140000, 159000),
    ("G&A", "Legal", "IC4"):                    (159000, 181000, 203000),
    ("G&A", "IT", "IC1"):                       (55000, 63000, 72000),
    ("G&A", "IT", "IC2"):                       (78000, 90000, 101000),
    ("G&A", "IT", "IC3"):                       (101000, 117000, 133000),
    ("G&A", "IT", "IC4"):                       (127000, 145000, 162000),
    ("G&A", "Facilities", "IC1"):               (48750, 56250, 63750),
    ("G&A", "Facilities", "IC2"):               (68000, 78000, 88000),
    ("G&A", "Facilities", "IC3"):               (88000, 102000, 116000),
    ("G&A", "Facilities", "IC4"):               (108000, 122000, 136000),
    ("G&A", "Finance", "M1"):                   (117000, 133000, 149000),
    ("G&A", "Legal", "M1"):                     (130000, 147500, 165000),
    ("G&A", "IT", "M1"):                        (117000, 133000, 149000),
    ("G&A", "Facilities", "M2"):                (140000, 155000, 170000),
    ("G&A", "Finance", "M4"):                   (238000, 271000, 304000),
    ("G&A", "(CFO)", "M5"):                     (320000, 380000, 440000),
    # People / HR
    ("People", "Recruiting", "IC1"):            (55000, 63000, 72000),
    ("People", "Recruiting", "IC2"):            (78000, 90000, 101000),
    ("People", "Recruiting", "IC3"):            (101000, 117000, 133000),
    ("People", "Recruiting", "IC4"):            (127000, 145000, 162000),
    ("People", "HRBPs", "IC1"):                 (70000, 80000, 90000),
    ("People", "HRBPs", "IC2"):                 (81000, 93000, 105000),
    ("People", "HRBPs", "IC3"):                 (105000, 121000, 138000),
    ("People", "HRBPs", "IC4"):                 (135000, 154000, 173000),
    ("People", "People Ops", "IC1"):            (52000, 60000, 68000),
    ("People", "People Ops", "IC2"):            (72000, 83000, 94000),
    ("People", "People Ops", "IC3"):            (94000, 109000, 124000),
    ("People", "Total Rewards", "IC1"):         (62000, 71000, 80000),
    ("People", "Total Rewards", "IC2"):         (85000, 97500, 110000),
    ("People", "Total Rewards", "IC3"):         (110000, 127500, 145000),
    ("People", "Total Rewards", "IC4"):         (140000, 160000, 178000),
    ("People", "L&D", "IC1"):                   (52000, 60000, 68000),
    ("People", "L&D", "IC2"):                   (72000, 83000, 94000),
    ("People", "L&D", "IC3"):                   (94000, 109000, 124000),
    ("People", "DEIB", "IC1"):                  (62000, 71000, 80000),
    ("People", "DEIB", "IC2"):                  (78000, 90000, 101000),
    ("People", "DEIB", "IC3"):                  (101000, 117000, 133000),
    ("People", "Recruiting", "M1"):             (117000, 133000, 149000),
    ("People", "HRBPs", "M1"):                  (120000, 136000, 152000),
    ("People", "L&D", "M1"):                    (110000, 125000, 140000),
    ("People", "DEIB", "M2"):                   (140000, 155000, 170000),
    ("People", "Recruiting", "M3"):             (185000, 208000, 231000),
    ("People", "People Ops", "M3"):             (185000, 208000, 231000),
    ("People", "(CPO)", "M5"):                  (304000, 361000, 418000),
    # Executive (CEO Maya)
    ("Executive", None, "M5"):                  (450000, 525000, 600000),
}

YEAR_MULTIPLIERS: dict[int, float] = {
    2020: 0.85, 2021: 0.88, 2022: 0.92, 2023: 0.95, 2024: 0.98, 2025: 1.00,
}

ARCHETYPE_HIRE_COMPA_RATIO: dict[str, tuple[float, float]] = {
    "High-flyer":                (1.05, 1.15),
    "Steady contributor":        (0.95, 1.05),
    "Early churner":             (0.90, 1.00),
    "Top performer flight risk": (1.00, 1.10),
    "Layoff casualty":           (0.95, 1.05),
    "Performance managed out":   (0.88, 0.98),
    "Internal mover":            (0.97, 1.03),
    "Manager step-back":         (1.00, 1.10),
    "Manager change casualty":   (0.95, 1.05),
    "Founder / early employee":  (1.10, 1.20),
    "Defined leadership":        (1.10, 1.25),
}

# Annual merit % per archetype (typical performance pattern). The
# archetype implicitly carries the modal performance rating; rare
# off-pattern years aren't simulated here.
ARCHETYPE_ANNUAL_MERIT: dict[str, tuple[float, float]] = {
    "High-flyer":                (0.04, 0.05),
    "Steady contributor":        (0.03, 0.04),
    "Early churner":             (0.00, 0.03),
    "Top performer flight risk": (0.04, 0.05),
    "Layoff casualty":           (0.02, 0.04),
    "Performance managed out":   (0.00, 0.01),
    "Internal mover":            (0.03, 0.04),
    "Manager step-back":         (0.03, 0.04),
    "Manager change casualty":   (0.03, 0.04),
    "Founder / early employee":  (0.04, 0.05),
    "Defined leadership":        (0.04, 0.05),
}

PROMOTION_BUMP_RANGE = (0.10, 0.15)
MARKET_ADJUSTMENT_BUMP_RANGE = (0.05, 0.08)
ANNUAL_REVIEW_MIN_TENURE_DAYS = 180  # 6 months


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_date(value) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _round_salary(salary: float) -> int:
    """Round to nearest $500."""
    return int(round(salary / 500) * 500)


def _lookup_band_2025(department: str, sub_department: str | None, level: str) -> tuple[int, int, int]:
    """Return the 2025 (min, mid, max) band for the cell.

    Cascade: (dept, sub_dept, level) -> (dept, "All", level) ->
    (dept, "All", same-level-class). Last fallback uses a generic
    base band; should not trigger for the cells in this dataset.
    """
    band = COMP_BANDS_2025.get((department, sub_department, level))
    if band:
        return band
    band = COMP_BANDS_2025.get((department, "All", level))
    if band:
        return band
    # Try other sub-depts at the same level within the dept (used as
    # fallback for cells like Customer Success / Implementation / IC5
    # that aren't explicit in Job Architecture).
    for (dept_key, _sd, lvl), candidate in COMP_BANDS_2025.items():
        if dept_key == department and lvl == level:
            return candidate
    # Last-resort fallback (should not trigger for current dataset).
    return (50000, 70000, 90000)


def _band_for_year(
    department: str, sub_department: str | None, level: str, year: int
) -> tuple[int, int, int]:
    """Apply the historical multiplier to the 2025 band."""
    base = _lookup_band_2025(department, sub_department, level)
    multiplier = YEAR_MULTIPLIERS.get(year, 1.00)
    return tuple(_round_salary(value * multiplier) for value in base)


def _draw_uniform(rng: random.Random, lo: float, hi: float) -> float:
    return rng.uniform(lo, hi)


def _clip_to_band(salary: int, band: tuple[int, int, int]) -> int:
    return max(band[0], min(band[2], salary))


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _load_state():
    """Run upstream stages and return (profiles, designations, raw_employees, raw_job_history)."""
    base = Path(__file__).parent
    stage1 = runpy.run_path(str(base / "01_generate_employee_profiles.py"), run_name="stage1")
    stage2a = runpy.run_path(str(base / "02_designate_manager_layer.py"), run_name="stage2a")
    stage2e = runpy.run_path(str(base / "07_materialize_raw_employees.py"), run_name="stage2e")
    stage3a = runpy.run_path(str(base / "08_complete_raw_job_history.py"), run_name="stage3a")

    return (
        stage1["build_employee_profiles"](),
        stage2a["build_level_designations"](),
        stage2e["build_raw_employees"](),
        stage3a["build_raw_job_history"](),
    )


# ---------------------------------------------------------------------------
# Per-employee comp event timeline
# ---------------------------------------------------------------------------

def _annual_review_dates(
    hire_date: date, end_date: date, promotion_dates: set[date]
) -> list[date]:
    """List Jan 15 dates eligible for an Annual Review row.

    Eligibility: at least 6 months tenure by Jan 15, profile still
    active by Jan 15, and the same calendar year did not already see a
    Promotion (to avoid double-bumping in one year). The 6-month rule
    means a Q4 hire skips the next Jan 15.
    """
    promo_years = {d.year for d in promotion_dates}
    dates: list[date] = []
    year = hire_date.year + 1
    cutoff = end_date
    while True:
        candidate = date(year, 1, 15)
        if candidate > cutoff:
            break
        tenure_days = (candidate - hire_date).days
        if tenure_days < ANNUAL_REVIEW_MIN_TENURE_DAYS:
            year += 1
            continue
        if year in promo_years:
            year += 1
            continue
        dates.append(candidate)
        year += 1
    return dates


def _gather_promotion_dates(
    job_history: pd.DataFrame, employee_id: str
) -> list[tuple[date, str]]:
    """Return [(effective_date, new_level), ...] for an employee's Promotion rows."""
    rows = job_history[
        (job_history["employee_id"] == employee_id)
        & (job_history["change_type"] == "Promotion")
    ].sort_values("effective_date")
    return [
        (_to_date(row["effective_date"]), row["new_job_level"])
        for _, row in rows.iterrows()
    ]


def _profile_end_date(profile: pd.Series) -> date:
    if profile["employment_status"] == "Active":
        return CURRENT_DATE
    return _to_date(profile["termination_date"])


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build_raw_compensation() -> pd.DataFrame:
    """Build the raw_compensation DataFrame."""
    profiles, designations, raw_employees, job_history = _load_state()
    rng = random.Random(RANDOM_SEED)

    # We need each employee's at-hire (department, sub_department, level)
    # and the same triple at each subsequent Promotion. The Hire row of
    # raw_job_history carries the at-hire triple; the Promotion rows
    # carry the new triple after each promotion (department and sub_dept
    # generally do not change at promotion time, but the level does, and
    # if a Lateral Transfer happened earlier the sub_dept may have moved
    # too -- raw_job_history's new_* columns track that).
    history_by_emp: dict[str, list[dict]] = defaultdict(list)
    for _, row in job_history.sort_values(
        ["employee_id", "effective_date"]
    ).iterrows():
        history_by_emp[row["employee_id"]].append(row.to_dict())

    rows: list[dict] = []

    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        archetype = profile["archetype"]
        hire_d = _to_date(profile["hire_date"])
        end_d = _profile_end_date(profile)
        if archetype not in ARCHETYPE_HIRE_COMPA_RATIO:
            archetype_for_comp = "Steady contributor"
        else:
            archetype_for_comp = archetype

        # Pull the chronological event log. Index 0 is always the Hire row.
        events = history_by_emp[emp_id]
        hire_row = events[0]
        assert hire_row["change_type"] == "Hire", (
            f"Expected Hire as first event for {emp_id}, got {hire_row['change_type']}"
        )

        starting_dept = hire_row["new_department"]
        starting_sub = hire_row["new_sub_department"]
        starting_level = hire_row["new_job_level"]

        # 1. New Hire compensation row.
        compa_lo, compa_hi = ARCHETYPE_HIRE_COMPA_RATIO[archetype_for_comp]
        compa_ratio = _draw_uniform(rng, compa_lo, compa_hi)
        band = _band_for_year(starting_dept, starting_sub, starting_level, hire_d.year)
        salary = _clip_to_band(_round_salary(band[1] * compa_ratio), band)
        rows.append({
            "employee_id":     emp_id,
            "salary":          salary,
            "comp_band_min":   band[0],
            "comp_band_mid":   band[1],
            "comp_band_max":   band[2],
            "effective_date":  hire_d,
            "change_reason":   "New Hire",
        })
        current_salary = salary
        current_dept = starting_dept
        current_sub = starting_sub
        current_level = starting_level

        # Build the comp event sequence: Promotions + Annual Reviews +
        # Market Adjustment, sorted chronologically. Lateral Transfer
        # rows update sub_dept (and thus the band lookup) but do not
        # produce a Pave row (no salary change). Manager Change rows
        # are likewise comp-neutral.
        comp_events: list[dict] = []

        # Walk the job_history events to apply state changes (sub_dept
        # via Lateral Transfer; level via Promotion / Title Change) and
        # to capture Promotion comp dates.
        for event in events[1:]:
            event_d = _to_date(event["effective_date"])
            change_type = event["change_type"]
            if change_type == "Promotion":
                comp_events.append({
                    "type": "Promotion",
                    "effective_date": event_d,
                    "new_level": event["new_job_level"],
                })
            # Lateral Transfer / Manager Change / Title Change comp
            # implications are handled inline when we walk comp_events:
            # we re-look up the band against the up-to-date sub_dept
            # and level when the comp event fires.

        promo_dates_only = {e["effective_date"] for e in comp_events if e["type"] == "Promotion"}

        for review_d in _annual_review_dates(hire_d, end_d, promo_dates_only):
            comp_events.append({
                "type": "Annual Review",
                "effective_date": review_d,
            })

        if hire_d < MARKET_ADJUSTMENT_DATE <= end_d:
            comp_events.append({
                "type": "Market Adjustment",
                "effective_date": MARKET_ADJUSTMENT_DATE,
            })

        comp_events.sort(key=lambda e: e["effective_date"])

        # Walk comp events chronologically. As we pass non-comp events
        # (Lateral Transfer / Title Change), keep current_sub /
        # current_level updated so the next comp lookup reflects them.
        history_iter = iter(events[1:])
        next_history = next(history_iter, None)

        for comp_event in comp_events:
            event_d = comp_event["effective_date"]

            # Roll forward all job_history events that occurred on or
            # before this comp event so current_* reflects the latest
            # state.
            while (
                next_history is not None
                and _to_date(next_history["effective_date"]) <= event_d
            ):
                if next_history["change_type"] == "Lateral Transfer":
                    current_sub = next_history["new_sub_department"]
                elif next_history["change_type"] == "Promotion":
                    current_level = next_history["new_job_level"]
                elif next_history["change_type"] == "Title Change":
                    # Manager Step-Back's M1 -> IC4. Salary stays per
                    # spec ("Retained IC4 band after step-back, no pay
                    # cut"); only the band used for subsequent comp
                    # events shifts.
                    current_level = next_history["new_job_level"]
                next_history = next(history_iter, None)

            year = event_d.year
            band = _band_for_year(current_dept, current_sub, current_level, year)

            if comp_event["type"] == "Promotion":
                bump = _draw_uniform(rng, *PROMOTION_BUMP_RANGE)
                new_salary = _clip_to_band(
                    _round_salary(current_salary * (1 + bump)), band
                )
                # Land at or above the new band's minimum even when the
                # 10-15% bump is small relative to the level jump.
                new_salary = max(new_salary, band[0])
                change_reason = "Promotion"
            elif comp_event["type"] == "Annual Review":
                merit_lo, merit_hi = ARCHETYPE_ANNUAL_MERIT[archetype_for_comp]
                bump = _draw_uniform(rng, merit_lo, merit_hi)
                if bump < 0.005:
                    # Effective freeze; spec says no row when comp is
                    # frozen. Skip.
                    continue
                new_salary = _clip_to_band(
                    _round_salary(current_salary * (1 + bump)), band
                )
                change_reason = "Annual Review"
            else:  # Market Adjustment
                bump = _draw_uniform(rng, *MARKET_ADJUSTMENT_BUMP_RANGE)
                new_salary = _clip_to_band(
                    _round_salary(current_salary * (1 + bump)), band
                )
                change_reason = "Market Adjustment"

            if new_salary == current_salary:
                continue  # no observable change; skip the row

            rows.append({
                "employee_id":     emp_id,
                "salary":          new_salary,
                "comp_band_min":   band[0],
                "comp_band_mid":   band[1],
                "comp_band_max":   band[2],
                "effective_date":  event_d,
                "change_reason":   change_reason,
            })
            current_salary = new_salary

    df = pd.DataFrame(rows, columns=COMPENSATION_COLUMNS)
    df = df.sort_values(["employee_id", "effective_date"]).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame) -> None:
    print(f"\n=== raw_compensation ===")
    print(f"  total rows:       {len(df)}")
    print(f"  unique employees: {df['employee_id'].nunique()}  (expected 604)")

    print("\n  By change_reason:")
    print(df["change_reason"].value_counts().to_string())

    print("\n  Rows per employee summary:")
    rows_per = df.groupby("employee_id").size()
    print(rows_per.describe().round(1).to_string())

    print("\n  Salary stats by change_reason:")
    print(
        df.groupby("change_reason")["salary"]
        .agg(["count", "min", "median", "max", "mean"])
        .round(0)
        .to_string()
    )

    print("\n  Compa-ratio (salary / band_mid) summary by change_reason:")
    df_calc = df.copy()
    df_calc["compa_ratio"] = df_calc["salary"] / df_calc["comp_band_mid"]
    print(
        df_calc.groupby("change_reason")["compa_ratio"]
        .agg(["min", "median", "max", "mean"])
        .round(3)
        .to_string()
    )

    print("\n  Hire-row coverage:")
    hires = df[df["change_reason"] == "New Hire"]
    print(f"    {len(hires)} of {df['employee_id'].nunique()} employees have a New Hire row")

    print("\n  Within-band check:")
    out_of_band = df[
        (df["salary"] < df["comp_band_min"])
        | (df["salary"] > df["comp_band_max"])
    ]
    print(f"    rows with salary outside [min, max]: {len(out_of_band)}")

    print("\n  Sample timeline (first employee with 5+ comp records):")
    multi = rows_per[rows_per >= 5].head(1).index
    if len(multi) > 0:
        sample = df[df["employee_id"].isin(multi)].sort_values(
            ["employee_id", "effective_date"]
        )
        print(sample.to_string(index=False))


if __name__ == "__main__":
    df = build_raw_compensation()
    print_summary(df)
