"""
Generate the master employee profile dataset for JustKaizen AI synthetic data.

Purpose
-------
Stage 1 of the synthetic data pipeline. Each profile carries the archetype
assignment, hire/termination state, and department placement that downstream
generators (raw_employees, raw_job_history, raw_compensation, raw_performance,
raw_recruiting) will derive their rows from. Generating profiles first, then
deriving every source-table row from them, is what enforces the cross-table
coherence rules (Section 12 of the spec).

Inputs
------
    Constants derived from `JustKaizen_AI_Data_Generation_Spec.md` (Sections
    1-5) and the Section 2 leadership table. No file I/O.

Outputs
-------
    `build_employee_profiles()` returns a pandas DataFrame containing the
    columns listed in `PROFILE_COLUMNS`. CSV materialization is intentionally
    deferred to a later stage so distributions can be reviewed first.

Pipeline placement
------------------
    Runs before every source-table generator. All six raw tables join back
    to these profiles via employee_id.

Headcount reconciliation
------------------------
    The spec's Section 5 archetype percentages do not algebraically reconcile
    with the stated 568 / 380 / 188 headcount. Counts below are the reviewed
    targets for this stage:

        Archetype                       Spec %   Count    Active   Term
        ---------                       ------   -----    ------   ----
        Defined leadership                 5%      19       18       1
        Founder / early employee           5%      27       27       0
        High-flyer                        15%      85       68      17
        Steady contributor                25%     214      214       0
        Early churner                     10%      35        0      35
        Top performer flight risk          8%      25        0      25
        Layoff casualty                   13%      75        0      75
        Performance managed out            4%      15        0      15
        Internal mover                     8%      45       45       0
        Manager step-back                  1%       8        8       0
        Manager change casualty            6%      20        0      20
        Total                                     568      380     188

    The active/terminated split for Layoff casualty (75 in Q1 2023) and the
    sole founder departure (Priya Sharma) are rigid spec constraints. The
    other terminated-archetype counts (Early churner, Top performer flight
    risk, Performance managed out, Manager change casualty) were sized so
    each bucket has enough records to surface visible patterns in dashboards.
    Those four plus High-flyer's 20% voluntary loss plus the layoff plus
    Priya total exactly 188, which leaves no termination budget for Steady
    Contributor (15% in spec) or Internal Mover (10% in spec). Both are
    therefore generated 100% active in this stage. Downstream visualizations
    should not expect voluntary turnover from those archetypes.

Hire-date assignment
--------------------
    Hire dates come from a 549-slot pool whose per-quarter shape mirrors
    Section 4 scaled by 549/470 (= number of non-leadership profiles
    divided by Section 4's gross-hire total). Archetypes are processed
    in narrowest-window-first order so that Steady Contributor, the only
    archetype with a 2021-2025 window, soaks up the leftover late-year
    slots that no other archetype can reach. Non-founder archetypes whose
    spec window starts in 2020 (High-flyer, Steady, Internal Mover,
    Manager Step-Back) are restricted to 2021+ to keep all 2020 slots
    available for the 9 Q1 2020 founder profiles plus the 4 leadership
    hires already placed in 2020 by Section 2.

Supplemental terminated profiles
--------------------------------
    After the base 568 profiles are generated, an additional 36 terminated
    profiles are appended to restore the spec's voluntary-turnover
    semantics for Steady Contributor (15% of 214 = 32) and Internal Mover
    (10% of 45 = 4). These bring totals to 604 / 380 / 224. The
    supplemental profiles are 100% terminated, hire 2021-2022 from a
    36-slot pool whose shape is Section 4 weighted within that window,
    and carry termination dates that are at least 6 months after hire
    and on or before 2025-03-31. Each is assigned a `manager_id`
    referencing an active M1+ employee in the same department (with
    cross-department fallback for early-2021 hires in departments that
    had no in-department M1+ active yet). This is the only column on
    which the supplemental profiles differ from the base; the base
    profiles' `manager_id` is left null at this stage and will be
    populated when raw_job_history and the M1-M2 manager layer are
    generated downstream.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import date, timedelta

import pandas as pd

# Deterministic generation. Bump the seed only when the spec or the
# reconciliation parameters change so the same input always produces the
# same dataset.
RANDOM_SEED = 20260425

PROFILE_COLUMNS = [
    "employee_id",
    "first_name",
    "last_name",
    "archetype",
    "department",
    "sub_department",
    "starting_job_level",
    "starting_job_title",
    "hire_date",
    "termination_date",
    "termination_type",
    "termination_reason",
    "employment_status",
    "is_leadership",
    "manager_id",
]

# ---------------------------------------------------------------------------
# Section 2 - Pre-defined leadership team
# ---------------------------------------------------------------------------
# Hard-coded because the spec assigns explicit names, employee_ids, hire
# dates, and reporting lines. Order matches Section 2 (C-suite, VP, Director).
LEADERSHIP_PROFILES = [
    # (employee_id, first, last, dept, sub_dept, level, title, hire_date, term_date, term_type, term_reason)
    ("EMP-001", "Maya",     "Chen",       "Executive",        None,                  "M5", "CEO",                            date(2020, 1, 15), None, None, None),
    ("EMP-002", "David",    "Okafor",     "Engineering",      "(CTO)",               "M5", "CTO",                            date(2020, 1, 15), None, None, None),
    ("EMP-003", "Marcus",   "Lee",        "Sales",            "(CRO)",               "M5", "CRO",                            date(2022, 2, 1),  None, None, None),
    ("EMP-004", "Aisha",    "Patel",      "G&A",              "(CFO)",               "M5", "CFO",                            date(2021, 6, 1),  None, None, None),
    ("EMP-005", "James",    "Wallace",    "Product",          "(CPO)",               "M5", "Chief Product Officer",          date(2023, 9, 15), None, None, None),
    ("EMP-006", "Rachel",   "Torres",     "People",           "(CPO)",               "M5", "Chief People Officer",           date(2023, 10, 1), None, None, None),
    ("EMP-007", "Kevin",    "Zhao",       "Engineering",      "Platform",            "M4", "VP Engineering, Platform",       date(2020, 6, 1),  None, None, None),
    ("EMP-008", "Amara",    "Johnson",    "Engineering",      "AI/ML",               "M4", "VP Engineering, AI/ML",          date(2021, 3, 1),  None, None, None),
    ("EMP-009", "Carlos",   "Mendez",     "Sales",            "Account Executive",   "M4", "VP Sales",                       date(2022, 4, 1),  None, None, None),
    ("EMP-010", "Lisa",     "Park",       "Customer Success", "(VP CS)",             "M4", "VP Customer Success",            date(2021, 9, 1),  None, None, None),
    ("EMP-011", "Nina",     "Okonkwo",    "Marketing",        "(VP Mktg)",           "M4", "VP Marketing",                   date(2021, 7, 1),  None, None, None),
    ("EMP-012", "Raj",      "Gupta",      "G&A",              "Finance",             "M4", "VP Finance",                     date(2021, 8, 1),  None, None, None),
    ("EMP-013", "Sarah",    "Kim",        "Engineering",      "Data",                "M3", "Director of Engineering, Data",  date(2021, 1, 15), None, None, None),
    ("EMP-014", "Jordan",   "Brooks",     "Engineering",      "Infrastructure",      "M3", "Director of Engineering, Infra", date(2021, 2, 1),  None, None, None),
    ("EMP-015", "Michelle", "Torres",     "People",           "Recruiting",          "M3", "Director of Recruiting",         date(2021, 5, 1),  None, None, None),
    ("EMP-016", "Derek",    "Washington", "People",           "People Ops",          "M3", "Director of People Ops",         date(2022, 1, 15), None, None, None),
    ("EMP-017", "Hannah",   "Lee",        "Product",          "Product Management",  "M3", "Director of Product",            date(2021, 11, 1), None, None, None),
    ("EMP-018", "Andre",    "Williams",   "Sales",            "Account Executive",   "M3", "Director of Sales, Enterprise",  date(2022, 5, 1),  None, None, None),
    ("EMP-019", "Priya",    "Sharma",     "Product",          "Product Management",  "M3", "Co-Founder / CPO",               date(2020, 1, 15), date(2023, 6, 30), "Voluntary", "Personal Reasons"),
]

# ---------------------------------------------------------------------------
# Active department targets (non-leadership)
# ---------------------------------------------------------------------------
# Section 3 row sums minus active leadership counted in those rows, then
# nudged upward to absorb the 8-employee gap between Section 3 row totals
# (371) and the spec's stated 380 active count. Keeps every department
# within +/- 3 of its Section 3 row sum.
NON_LEADERSHIP_ACTIVE_DEPT_TARGETS: dict[str, int] = {
    "Engineering":      137,
    "Sales":             70,
    "Customer Success":  39,
    "Marketing":         29,
    "Product":           24,
    "G&A":               33,
    "People":            30,
}

# ---------------------------------------------------------------------------
# Section 1 - Q1 2023 layoff distribution (75 people total)
# ---------------------------------------------------------------------------
LAYOFF_DEPT_DISTRIBUTION: dict[str, int] = {
    "Sales":            25,
    "Engineering":      20,
    "G&A":              12,
    "Marketing":         8,
    "Customer Success":  5,
    "People":            5,
    "Product":           0,
}

# ---------------------------------------------------------------------------
# Section 4 - Quarterly hiring volume by department
# ---------------------------------------------------------------------------
# Drives both the hire-date sampling distribution (sum across departments
# per quarter) and the historical-hiring weights used for placing
# non-layoff terminated profiles into a department. Quarters with zero
# total hires (2020 Q2-Q4, 2023 Q1-Q2) are omitted because they contribute
# nothing.
QUARTERLY_HIRES: list[tuple[int, int, int, int, int, int, int, int, int]] = [
    # (year, quarter, eng, sales, cs, mktg, product, ga, people)
    (2020, 1,  5,  0, 0, 0, 3, 2, 2),
    (2021, 1, 10,  3, 2, 2, 3, 2, 1),
    (2021, 2, 15,  6, 4, 3, 3, 2, 2),
    (2021, 3, 22, 10, 5, 4, 3, 3, 3),
    (2021, 4, 25, 15, 6, 5, 3, 3, 3),
    (2022, 1, 28, 18, 7, 6, 4, 4, 3),
    (2022, 2, 30, 22, 8, 7, 4, 5, 4),
    (2022, 3, 22, 16, 6, 5, 3, 4, 4),
    (2022, 4, 10,  8, 3, 3, 2, 2, 2),
    (2023, 3,  4,  2, 1, 1, 1, 0, 1),
    (2023, 4,  2,  1, 1, 0, 0, 1, 0),
    (2024, 1,  2,  1, 1, 0, 1, 0, 0),
    (2024, 2,  3,  1, 1, 1, 0, 1, 0),
    (2024, 3,  2,  1, 1, 0, 1, 0, 0),
    (2024, 4,  3,  2, 1, 1, 0, 1, 0),
    (2025, 1,  3,  2, 1, 1, 1, 1, 1),
]

DEPT_ORDER = ["Engineering", "Sales", "Customer Success", "Marketing", "Product", "G&A", "People"]

# Non-leadership hire-slot pool, scaled from Section 4 to total exactly 549
# (= 568 total profiles - 19 leadership profiles). The 2020 Q1 row carries
# only 9 slots so the 9 Q1 2020 founder profiles fit exactly there with no
# overflow into other 2020 quarters; the leadership block separately
# occupies 4 hires in 2020 (Maya, David, Priya, Kevin) which are not drawn
# from this pool. Non-Q1-2020 quarters are scaled by 540 / 458 = 1.1790
# rounded to integers, with rounding biased upward so the pool sums to 549.
NON_LEADERSHIP_HIRE_SLOTS: dict[tuple[int, int], int] = {
    (2020, 1): 9,
    (2021, 1): 27, (2021, 2): 41, (2021, 3): 59, (2021, 4): 71,
    (2022, 1): 83, (2022, 2): 94, (2022, 3): 71, (2022, 4): 35,
    (2023, 3): 12, (2023, 4): 6,
    (2024, 1): 6,  (2024, 2): 8,  (2024, 3): 6,  (2024, 4): 9,
    (2025, 1): 12,
}

# Archetype processing order. Tightest hire window first so the loosest
# (Steady Contributor, 2021-2025) gets the leftover slots and ends up
# absorbing the late-year quarters that no other archetype can reach.
ARCHETYPE_PROCESSING_ORDER = [
    "Founder / early employee",
    "Layoff casualty",
    "High-flyer",
    "Top performer flight risk",
    "Manager step-back",
    "Performance managed out",
    "Manager change casualty",
    "Internal mover",
    "Early churner",
    "Steady contributor",
]

# ---------------------------------------------------------------------------
# Supplemental terminated profiles (added after base generation)
# ---------------------------------------------------------------------------
# Restore the spec's voluntary-turnover patterns for Steady Contributor
# (15% of 214 = 32) and Internal Mover (10% of 45 = 4) without disturbing
# the base 568-profile dataset. All supplemental profiles are 100%
# terminated and constrained to 2021-2022 hires per user direction.
SUPPLEMENTAL_TERMINATIONS: dict[str, dict] = {
    "Steady contributor": {
        "count": 32,
        "starting_levels": ("IC1", "IC2", "IC3"),
        "termination_reasons": {
            "Compensation": 0.30,
            "Work-Life Balance": 0.25,
            "Role Misalignment": 0.20,
            "Relocation": 0.15,
            "Personal Reasons": 0.10,
        },
    },
    "Internal mover": {
        "count": 4,
        "starting_levels": ("IC2", "IC3", "IC4"),
        "termination_reasons": {
            "Career Opportunity": 0.30,
            "Compensation": 0.30,
            "Work-Life Balance": 0.20,
            "Role Misalignment": 0.20,
        },
    },
}

# 36 supplemental hire slots distributed across 2021-2022 with shape
# matching Section 4 (largest-remainder rounding of 36 * Q_share /
# 408_total). Sums to exactly 36.
SUPPLEMENTAL_HIRE_SLOTS: dict[tuple[int, int], int] = {
    (2021, 1): 2, (2021, 2): 3, (2021, 3): 5, (2021, 4): 5,
    (2022, 1): 6, (2022, 2): 7, (2022, 3): 5, (2022, 4): 3,
}

MANAGER_LEVELS = frozenset({"M1", "M2", "M3", "M4", "M5"})

SUB_DEPT_BY_DEPT: dict[str, list[str]] = {
    "Engineering":      ["Platform", "AI/ML", "Data", "Infrastructure"],
    "Sales":            ["SDR", "Account Executive", "Account Management", "Sales Engineering"],
    "Customer Success": ["CSM", "Support", "Implementation"],
    "Marketing":        ["Growth", "Content", "Product Marketing", "Brand"],
    "Product":          ["Product Management", "Design", "UX Research"],
    "G&A":              ["Finance", "Legal", "IT", "Facilities"],
    "People":           ["HRBPs", "Recruiting", "People Ops", "Total Rewards", "L&D", "DEIB"],
}


@dataclass
class ArchetypeSpec:
    """One archetype's count, status mix, hire window, level range, and termination distributions."""

    name: str
    target_count: int
    active_share: float                   # 0.0 - 1.0 share of profiles that remain active
    hire_year_min: int
    hire_year_max: int
    starting_levels: tuple[str, ...]
    term_type_distribution: dict[str, float] = field(default_factory=dict)
    term_reason_distribution: dict[str, float] = field(default_factory=dict)


# Counts and active rates reconciled to 568 / 380 / 188. See module docstring
# for the rationale; in particular Steady Contributor and Internal Mover are
# 100% active because the user-reviewed terminated-archetype counts already
# saturate the 188 termination budget.
ARCHETYPE_SPECS: list[ArchetypeSpec] = [
    # hire_year_min is 2021 (not 2020 per spec) for High-flyer, Steady,
    # Internal Mover, and Manager Step-Back so the 12 Section 4 slots in
    # Q1 2020 are reserved for the 3 Q1 2020 leadership profiles plus the
    # 9 founder profiles. The spec's wider windows on these archetypes
    # implied 2020 hires that conflict with the company's actual 12-person
    # 2020 headcount.
    ArchetypeSpec(
        "High-flyer", 85, 0.80, 2021, 2022, ("IC2", "IC3"),
        {"Voluntary": 1.0},
        {"Career Opportunity": 1.0},
    ),
    ArchetypeSpec(
        "Steady contributor", 214, 1.0, 2021, 2025, ("IC1", "IC2", "IC3"),
        {}, {},
    ),
    ArchetypeSpec(
        "Early churner", 35, 0.0, 2022, 2024, ("IC1", "IC2"),
        {"Voluntary": 1.0},
        {"Role Misalignment": 0.35, "Company Culture": 0.25,
         "Compensation": 0.20, "Personal Reasons": 0.20},
    ),
    ArchetypeSpec(
        "Top performer flight risk", 25, 0.0, 2021, 2022, ("IC2", "IC3"),
        {"Voluntary": 1.0},
        {"Career Opportunity": 1.0},
    ),
    ArchetypeSpec(
        "Layoff casualty", 75, 0.0, 2021, 2022, ("IC1", "IC2", "IC3"),
        {"Layoff": 1.0},
        {"Reduction in Force": 1.0},
    ),
    ArchetypeSpec(
        "Performance managed out", 15, 0.0, 2021, 2023, ("IC1", "IC2", "IC3"),
        {"Involuntary": 1.0},
        {"Performance": 1.0},
    ),
    ArchetypeSpec(
        "Internal mover", 45, 1.0, 2021, 2023, ("IC2", "IC3", "IC4"),
        {}, {},
    ),
    ArchetypeSpec(
        "Manager step-back", 8, 1.0, 2021, 2022, ("IC3",),
        {}, {},
    ),
    ArchetypeSpec(
        "Manager change casualty", 20, 0.0, 2021, 2023, ("IC2", "IC3", "IC4"),
        {"Voluntary": 1.0},
        {"Manager Relationship": 0.50, "Company Culture": 0.30,
         "Career Opportunity": 0.20},
    ),
    ArchetypeSpec(
        "Founder / early employee", 27, 1.0, 2020, 2021, ("IC3", "IC4", "M1"),
        {}, {},
    ),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _quarter_window(year: int, quarter: int) -> tuple[date, date]:
    """Return inclusive (start_date, end_date) for the given calendar quarter."""
    starts = {1: (1, 1), 2: (4, 1), 3: (7, 1), 4: (10, 1)}
    ends = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
    return date(year, *starts[quarter]), date(year, *ends[quarter])


def _historical_hires_by_dept() -> dict[str, int]:
    """Return Section 4 cumulative hire counts per department.

    Used to weight the department of non-layoff terminated profiles, on the
    assumption that voluntary attrition is proportional to historical hiring
    volume.
    """
    totals = {dept: 0 for dept in DEPT_ORDER}
    for _year, _quarter, eng, sales, cs, mktg, product, ga, people in QUARTERLY_HIRES:
        totals["Engineering"] += eng
        totals["Sales"] += sales
        totals["Customer Success"] += cs
        totals["Marketing"] += mktg
        totals["Product"] += product
        totals["G&A"] += ga
        totals["People"] += people
    return totals


def _random_date_in_window(rng: random.Random, start: date, end: date) -> date:
    """Pick a uniform random calendar date in [start, end]."""
    span = (end - start).days
    return start + timedelta(days=rng.randint(0, span))


def _claim_hire_slot(
    rng: random.Random,
    slot_pool: dict[tuple[int, int], int],
    year_min: int,
    year_max: int,
    quarter_filter: set[tuple[int, int]] | None = None,
) -> date:
    """Consume one hire slot from the pool and return a random date in that quarter.

    Eligible quarters are those inside [year_min, year_max] with remaining
    capacity, optionally restricted by `quarter_filter`. The chosen quarter
    is drawn weighted by its remaining slot count, which produces a
    Section-4-shaped distribution while guaranteeing each quarter is filled
    no more than its allotted capacity. `slot_pool` is mutated in place.
    """
    eligible = [
        ((y, q), c) for (y, q), c in slot_pool.items()
        if year_min <= y <= year_max
        and c > 0
        and (quarter_filter is None or (y, q) in quarter_filter)
    ]
    if not eligible:
        raise ValueError(
            f"Hire slot pool exhausted in window {year_min}-{year_max} "
            f"with filter {quarter_filter}; remaining pool: "
            f"{ {k: v for k, v in slot_pool.items() if v > 0} }"
        )
    keys = [k for k, _ in eligible]
    weights = [c for _, c in eligible]
    year, quarter = rng.choices(keys, weights=weights, k=1)[0]
    slot_pool[(year, quarter)] -= 1
    start, end = _quarter_window(year, quarter)
    return _random_date_in_window(rng, start, end)


def _weighted_choice(rng: random.Random, distribution: dict[str, float]) -> str:
    """Sample one key from `distribution` weighted by its value."""
    keys = list(distribution.keys())
    weights = list(distribution.values())
    return rng.choices(keys, weights=weights, k=1)[0]


def _draw_termination_date(rng: random.Random, archetype: str, hire_date: date) -> date:
    """Draw a termination date appropriate for the archetype's tenure pattern."""
    cutoff = date(2025, 3, 31)
    if archetype == "Early churner":
        # Spec: 2-11 months tenure
        days = rng.randint(60, 330)
    elif archetype == "Top performer flight risk":
        # Spec: 18-24 months since last promotion. Approximated as 24-36 months tenure.
        days = rng.randint(720, 1080)
    elif archetype == "Performance managed out":
        # Spec: departed 1-3 months after a final review cycle. 12-30 months tenure.
        days = rng.randint(360, 900)
    elif archetype == "Manager change casualty":
        # Spec: within 6 months of manager change. Approximated as 12-30 months tenure.
        days = rng.randint(360, 900)
    elif archetype == "High-flyer":
        # Spec: left after 18+ months without next promotion. 24-48 months tenure.
        days = rng.randint(720, 1440)
    else:
        days = rng.randint(180, 1080)
    candidate = hire_date + timedelta(days=days)
    if candidate > cutoff:
        candidate = cutoff
    if candidate <= hire_date:
        candidate = hire_date + timedelta(days=30)
    return candidate


def _next_employee_id(counter: list[int]) -> str:
    """Mutable counter helper. Returns the next EMP-#### id in zero-padded sequence."""
    counter[0] += 1
    return f"EMP-{counter[0]:04d}"


def _validate_archetype_math() -> None:
    """Fail loudly if archetype targets do not reconcile to 568 / 380 / 188."""
    total = len(LEADERSHIP_PROFILES) + sum(s.target_count for s in ARCHETYPE_SPECS)
    leadership_active = sum(1 for p in LEADERSHIP_PROFILES if p[8] is None)
    archetype_active = sum(round(s.target_count * s.active_share) for s in ARCHETYPE_SPECS)
    active = leadership_active + archetype_active
    assert total == 568, f"Archetype targets sum to {total}, expected 568"
    assert active == 380, f"Active count {active} does not match expected 380"

    non_leadership_active_total = sum(NON_LEADERSHIP_ACTIVE_DEPT_TARGETS.values())
    assert non_leadership_active_total == archetype_active, (
        f"Non-leadership active targets sum to {non_leadership_active_total}, "
        f"need to match archetype active {archetype_active}"
    )

    slot_total = sum(NON_LEADERSHIP_HIRE_SLOTS.values())
    non_leadership_total = sum(s.target_count for s in ARCHETYPE_SPECS)
    assert slot_total == non_leadership_total, (
        f"Hire slot pool totals {slot_total} but non-leadership profile count "
        f"is {non_leadership_total}"
    )

    archetype_names = {s.name for s in ARCHETYPE_SPECS}
    processing_set = set(ARCHETYPE_PROCESSING_ORDER)
    assert archetype_names == processing_set, (
        f"ARCHETYPE_PROCESSING_ORDER {processing_set} does not cover "
        f"ARCHETYPE_SPECS {archetype_names}"
    )

    supplemental_count = sum(c["count"] for c in SUPPLEMENTAL_TERMINATIONS.values())
    supplemental_slots = sum(SUPPLEMENTAL_HIRE_SLOTS.values())
    assert supplemental_slots == supplemental_count, (
        f"Supplemental hire slot pool totals {supplemental_slots}, "
        f"need to match {supplemental_count} supplemental profiles"
    )


def _generate_supplemental_terminated_profiles(
    rng: random.Random,
    base_profiles: pd.DataFrame,
    id_counter: list[int],
) -> list[dict]:
    """Generate the 32 Steady + 4 Internal Mover terminated profiles.

    These are appended after the base 568 are built. Hire dates draw from
    a 36-slot pool over 2021-2022 (Section 4 weighted within that window).
    Termination dates are at least 6 months after hire and on or before
    2025-03-31. `manager_id` references an active M1+ employee from
    `base_profiles` who works in the same department and was hired before
    this profile; if no in-department candidate exists (typical for early
    2021 hires in CS / Marketing / People where the senior layer was not
    yet built out), the assignment falls back to any active M1+ employee
    hired before this profile.
    """
    same_dept_candidates: dict[str, list[tuple[str, date]]] = {}
    for _, row in base_profiles[
        (base_profiles["employment_status"] == "Active")
        & (base_profiles["starting_job_level"].isin(MANAGER_LEVELS))
    ].iterrows():
        hire = row["hire_date"]
        if isinstance(hire, pd.Timestamp):
            hire = hire.date()
        same_dept_candidates.setdefault(row["department"], []).append(
            (row["employee_id"], hire)
        )
    all_candidates: list[tuple[str, date]] = [
        c for candidates in same_dept_candidates.values() for c in candidates
    ]

    supplemental_slots = dict(SUPPLEMENTAL_HIRE_SLOTS)
    historical_dept_hires = _historical_hires_by_dept()
    historical_dept_keys = list(historical_dept_hires.keys())
    historical_dept_weights = list(historical_dept_hires.values())
    cutoff = date(2025, 3, 31)

    new_rows: list[dict] = []
    for archetype_name, config in SUPPLEMENTAL_TERMINATIONS.items():
        for _ in range(config["count"]):
            hire_date = _claim_hire_slot(rng, supplemental_slots, 2021, 2022)

            # Department: weight by historical hiring volume, same as
            # other voluntary-departure archetypes in the base block.
            dept = rng.choices(historical_dept_keys, weights=historical_dept_weights, k=1)[0]

            # Manager: prefer same-department M1+ active and hired before
            # this profile; fall back cross-department if none.
            in_dept = [
                emp_id for emp_id, mgr_hire in same_dept_candidates.get(dept, [])
                if mgr_hire < hire_date
            ]
            if in_dept:
                manager_id = rng.choice(in_dept)
            else:
                cross_dept = [
                    emp_id for emp_id, mgr_hire in all_candidates
                    if mgr_hire < hire_date
                ]
                manager_id = rng.choice(cross_dept) if cross_dept else None

            min_term = hire_date + timedelta(days=180)
            term_date = _random_date_in_window(rng, min_term, cutoff)
            term_reason = _weighted_choice(rng, config["termination_reasons"])

            new_rows.append({
                "employee_id": _next_employee_id(id_counter),
                "first_name": None,
                "last_name": None,
                "archetype": archetype_name,
                "department": dept,
                "sub_department": rng.choice(SUB_DEPT_BY_DEPT[dept]),
                "starting_job_level": rng.choice(config["starting_levels"]),
                "starting_job_title": None,
                "hire_date": hire_date,
                "termination_date": term_date,
                "termination_type": "Voluntary",
                "termination_reason": term_reason,
                "employment_status": "Terminated",
                "is_leadership": False,
                "manager_id": manager_id,
            })

    leftover = {k: v for k, v in supplemental_slots.items() if v != 0}
    assert not leftover, f"Supplemental slot pool not fully consumed: {leftover}"
    return new_rows


# ---------------------------------------------------------------------------
# Profile builder
# ---------------------------------------------------------------------------

def build_employee_profiles() -> pd.DataFrame:
    """Return a DataFrame of all 568 employee profiles with archetype assignments.

    Department assignment is exact for active profiles (drawn from a pre-built
    slot pool sized to the per-department targets) and exact for layoff
    casualties (drawn from the Section 1 layoff distribution). Non-layoff
    terminated profiles are sampled from Section 4 historical hiring weights.
    Hire dates are drawn from Section 4 quarter weights, filtered to each
    archetype's allowed window, so the company's quarterly hiring ramp is
    preserved instead of uniformly smeared across years.
    """
    _validate_archetype_math()
    rng = random.Random(RANDOM_SEED)

    historical_dept_hires = _historical_hires_by_dept()
    historical_dept_keys = list(historical_dept_hires.keys())
    historical_dept_weights = list(historical_dept_hires.values())

    # Pre-build the non-leadership hire-slot pool. Consumed by archetypes
    # in tightest-window-first order so Steady Contributor, the only
    # archetype with a 2021-2025 window, soaks up the leftover late-year
    # slots that no other archetype can reach.
    slot_pool: dict[tuple[int, int], int] = dict(NON_LEADERSHIP_HIRE_SLOTS)

    # Pre-build the active department slot pool (one entry per active
    # non-leadership profile; consumed in shuffled order). This guarantees
    # active department counts land within +/- 0 of the targets, before
    # the leadership block bumps them to within +/- 3 of Section 3.
    active_dept_pool: list[str] = []
    for dept, count in NON_LEADERSHIP_ACTIVE_DEPT_TARGETS.items():
        active_dept_pool.extend([dept] * count)
    rng.shuffle(active_dept_pool)
    active_dept_iter = iter(active_dept_pool)

    # Pre-build the layoff department slot pool (75 entries per Section 1).
    layoff_dept_pool: list[str] = []
    for dept, count in LAYOFF_DEPT_DISTRIBUTION.items():
        layoff_dept_pool.extend([dept] * count)
    rng.shuffle(layoff_dept_pool)
    layoff_dept_iter = iter(layoff_dept_pool)

    # Layoff termination dates spread across Jan 15 - Mar 15, 2023 (Section 5.5).
    layoff_term_dates = [
        _random_date_in_window(rng, date(2023, 1, 15), date(2023, 3, 15))
        for _ in range(75)
    ]
    layoff_term_iter = iter(layoff_term_dates)

    rows: list[dict] = []
    id_counter = [19]  # Next id after the leadership block is EMP-0020.

    # 1. Leadership profiles -- copied verbatim from the spec.
    for (
        emp_id, first, last, dept, sub_dept, level, title,
        hire_date, term_date, term_type, term_reason,
    ) in LEADERSHIP_PROFILES:
        rows.append({
            "employee_id": emp_id,
            "first_name": first,
            "last_name": last,
            "archetype": "Defined leadership",
            "department": dept,
            "sub_department": sub_dept,
            "starting_job_level": level,
            "starting_job_title": title,
            "hire_date": hire_date,
            "termination_date": term_date,
            "termination_type": term_type,
            "termination_reason": term_reason,
            "employment_status": "Terminated" if term_date else "Active",
            "is_leadership": True,
            "manager_id": None,
        })

    # 2. Non-leadership archetypes, processed tight-window-first.
    archetype_specs_by_name = {s.name: s for s in ARCHETYPE_SPECS}

    for archetype_name in ARCHETYPE_PROCESSING_ORDER:
        spec = archetype_specs_by_name[archetype_name]
        active_count = round(spec.target_count * spec.active_share)
        is_layoff_archetype = spec.name == "Layoff casualty"
        is_founder_archetype = spec.name == "Founder / early employee"

        for i in range(spec.target_count):
            is_active = i < active_count

            # ---- Hire date (claims one slot from the pool) ----
            if is_founder_archetype:
                # Spec says "all hired Q1 2020", but the slot pool only
                # holds 9 Q1 2020 slots (Section 4 = 12 minus 3 leadership
                # already placed there). The first 9 founder profiles fill
                # those exactly; the remaining 18 are placed in 2021 H1,
                # the earliest available "early employee" window.
                if i < 9:
                    hire_date = _claim_hire_slot(
                        rng, slot_pool, 2020, 2020, quarter_filter={(2020, 1)},
                    )
                    # Q1 2020 C-suite (Maya, David, Priya) hired 2020-01-15.
                    # Clamp founders to Jan 16+ so the founding cohort joins
                    # after the C-suite founders, not before; otherwise the
                    # ~16% probability of landing Jan 1-14 leaves these
                    # profiles with no valid manager during 2b's hierarchy
                    # resolution. This is a post-claim clamp (no extra rng
                    # calls) so downstream RNG state is unchanged.
                    if hire_date < date(2020, 1, 16):
                        hire_date = date(2020, 1, 16)
                else:
                    hire_date = _claim_hire_slot(
                        rng, slot_pool, 2021, 2021,
                        quarter_filter={(2021, 1), (2021, 2)},
                    )
            else:
                hire_date = _claim_hire_slot(
                    rng, slot_pool, spec.hire_year_min, spec.hire_year_max,
                )

            # ---- Department ----
            if is_layoff_archetype:
                dept = next(layoff_dept_iter)
            elif is_active:
                dept = next(active_dept_iter)
            else:
                # Terminated non-layoff: weight by historical hiring volume.
                dept = rng.choices(historical_dept_keys, weights=historical_dept_weights, k=1)[0]

            sub_dept = rng.choice(SUB_DEPT_BY_DEPT[dept])

            # ---- Termination ----
            if is_active:
                term_date = None
                term_type = None
                term_reason = None
            elif is_layoff_archetype:
                term_date = next(layoff_term_iter)
                term_type = "Layoff"
                term_reason = "Reduction in Force"
            else:
                term_date = _draw_termination_date(rng, spec.name, hire_date)
                term_type = (
                    _weighted_choice(rng, spec.term_type_distribution)
                    if spec.term_type_distribution else "Voluntary"
                )
                term_reason = (
                    _weighted_choice(rng, spec.term_reason_distribution)
                    if spec.term_reason_distribution else "Personal Reasons"
                )

            rows.append({
                "employee_id": _next_employee_id(id_counter),
                "first_name": None,
                "last_name": None,
                "archetype": spec.name,
                "department": dept,
                "sub_department": sub_dept,
                "starting_job_level": rng.choice(spec.starting_levels),
                "starting_job_title": None,
                "hire_date": hire_date,
                "termination_date": term_date,
                "termination_type": term_type,
                "termination_reason": term_reason,
                "employment_status": "Terminated" if term_date else "Active",
                "is_leadership": False,
                "manager_id": None,
            })

    # Sanity check: every base slot should have been consumed.
    leftover = {k: v for k, v in slot_pool.items() if v != 0}
    assert not leftover, f"Hire slot pool not fully consumed: {leftover}"

    # Materialize the base 568 profiles before generating supplemental ones,
    # because the supplemental generator needs to look up active M1+
    # candidates per department from the assembled dataframe.
    base_df = pd.DataFrame(rows, columns=PROFILE_COLUMNS)
    base_df["hire_date"] = pd.to_datetime(base_df["hire_date"])
    base_df["termination_date"] = pd.to_datetime(base_df["termination_date"])

    supplemental_rows = _generate_supplemental_terminated_profiles(rng, base_df, id_counter)
    supplemental_df = pd.DataFrame(supplemental_rows, columns=PROFILE_COLUMNS)
    supplemental_df["hire_date"] = pd.to_datetime(supplemental_df["hire_date"])
    supplemental_df["termination_date"] = pd.to_datetime(supplemental_df["termination_date"])

    df = pd.concat([base_df, supplemental_df], ignore_index=True)
    df["hire_year"] = df["hire_date"].dt.year
    return df


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_distribution_summary(profiles: pd.DataFrame) -> None:
    """Print the four review tables: archetype, department, status, hire year."""
    print(f"\nTotal profiles generated: {len(profiles)}")
    print(f"Active:                   {(profiles['employment_status'] == 'Active').sum()}")
    print(f"Terminated:               {(profiles['employment_status'] == 'Terminated').sum()}")

    print("\n--- Distribution by Archetype ---")
    by_archetype = (
        profiles.groupby("archetype")
        .agg(total=("employee_id", "count"),
             active=("employment_status", lambda s: (s == "Active").sum()),
             terminated=("employment_status", lambda s: (s == "Terminated").sum()))
        .sort_values("total", ascending=False)
    )
    print(by_archetype.to_string())

    print("\n--- Distribution by Department ---")
    by_dept = (
        profiles.groupby("department")
        .agg(total=("employee_id", "count"),
             active=("employment_status", lambda s: (s == "Active").sum()),
             terminated=("employment_status", lambda s: (s == "Terminated").sum()))
        .sort_values("total", ascending=False)
    )
    print(by_dept.to_string())

    print("\n--- Distribution by Employment Status ---")
    print(profiles["employment_status"].value_counts().to_string())

    print("\n--- Distribution by Hire Year ---")
    by_year = (
        profiles.groupby("hire_year")
        .agg(total=("employee_id", "count"),
             active=("employment_status", lambda s: (s == "Active").sum()),
             terminated=("employment_status", lambda s: (s == "Terminated").sum()))
        .sort_index()
    )
    print(by_year.to_string())


if __name__ == "__main__":
    employee_profiles = build_employee_profiles()
    print_distribution_summary(employee_profiles)
