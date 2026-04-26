"""
Substage 2c (step 2): Sub-department x level grid alignment.

Purpose
-------
Reassign active flexible profiles (Steady Contributor, High-flyer
non-manager, Internal Mover non-manager) so the (department,
sub_department, current_job_level) distribution matches the Section 3
target grid identified in step 1's audit. Each promotion is recorded
so 2e can later materialize raw_job_history rows and raw_compensation
records. Carlos Mendez (VP Sales) is moved from Sales/Account
Executive to a Sales/(VP Sales) placeholder sub-department, mirroring
how the spec handles VP CS, VP Marketing, etc.

Inputs
------
- Stage 1: build_employee_profiles()
- Stage 2a: build_level_designations()

Outputs
-------
build_aligned_grid() returns three DataFrames:
  1. assignments_df          one row per profile with starting + final
                             sub_department and job_level, plus
                             promotion_count and subdept_changed flags.
  2. promotion_events_df     one row per single-step promotion for the
                             flex pool. 2e materializes these into
                             raw_job_history (Promotion) + raw_compensation
                             (Promotion bump) records.
  3. subdept_change_events_df one row per lateral transfer from this
                             stage's rebalancing. The Carlos correction
                             is NOT recorded as a transfer (it is a
                             stage-1 placement fix, not a real move).

Rules
-----
Archetype promotion budgets (active profiles only):
  High-flyer:           up to 3 promotions
  Steady contributor:   up to 1 promotion
  Internal mover:       up to 1 promotion
  Manager Step-Back:    locked at IC4 (archetype rule)

Flex promotions never cross into the manager track (cap at IC5). The
manager-track profiles' promotion histories are derived from 2a's
designations directly in 2e.

Locked-from-step-2 cohorts:
  - 19 Defined-leadership profiles (sub-dept fixed by spec, level
    fixed by Section 2; Carlos relocated to (VP Sales))
  - 49 Stage 2a manager designations (M1 / M2 + Stage 1 sub-dept,
    locked here -- 2e re-resolves manager_id against this grid)
  - 10 Stage 2a founder IC designations (IC4 / IC5 + Stage 1 sub-dept)
  - 8 Manager Step-Back (current_level = IC4, sub-dept from Stage 1)

The +8 active-headcount uplift (1c, to hit 380 active vs Section 3's
372 with Maya) is distributed across the high-volume sub-departments
in each department's IC3 cell. Carlos's relocation absorbs one of
Sales's two uplift slots; the other goes to Sales/Account Executive
IC3 per the spec's Sales staffing pattern.

Known spec artifacts (13 residual delta cells)
----------------------------------------------
After the alignment runs, 13 cells remain off the Section 3 + uplift
grid. They are intentional; documented here so a reviewer of the
final raw_employees output can find the explanation.

The greedy assignment also enforces a no-demotion guard: a flex
profile is never placed at a level lower than its Stage 1 starting
level. Without the guard, profiles starting at e.g. IC4 would beat
IC1-starting profiles for an open IC1 slot (their negative
promos_needed would dominate the sort), producing 10 demotions in
the resulting raw_job_history Hire rows. Preventing demotions
exchanges those 10 erroneous level changes for a few extra residual
delta cells (largely concentrated in Sales SDR IC1 where the demand
of 10 IC1 seats exceeds the flex pool's IC1-starting supply once
demotions are excluded).

    +1  Customer Success / Implementation / IC5
    -1  Customer Success / Support       / IC1
    +1  People           / HRBPs         / IC1
    -1  People           / L&D           / M1
    +1  Product          / Design        / IC5
    +1  Product          / UX Research   / IC1
    -2  Product          / UX Research   / IC3

Cause 1 -- Founder IC track at IC5 outside Engineering. Stage 2a's
founder IC track produces 5 IC5 designations distributed by tenure
across the founder departments. Section 3 only has IC5 cells in
Engineering. When an IC5 founder lands in Customer Success or Product,
Section 3 cannot accept the IC5 level and the profile spills into a
non-Section-3 cell (e.g. CS Implementation IC5, Product Design IC5).
Resolving this would require either reassigning founders cross-dept
(violates Stage 1 dept placement) or extending Section 3 with IC5
cells in non-Engineering departments (deviates from spec).

Cause 2 -- People L&D M1 vacancy. Section 3 row totals imply 4 M1 in
People (HRBPs / Recruiting / People Ops / L&D). Section 2's manager
allocation says People = 3 M1 + 1 M2. Stage 2a designated 3 M1 per
Section 2, leaving the Section 3 L&D M1 cell vacant. The spillover
flex profile in People sits at HRBPs IC1.

Cause 3 -- Product flex shortage. Product has 26 active total; after
locked profiles consume 8 (2 leadership + 4 manager designations + 1
founder IC + 1 step-back), only 18 flex profiles remain to fill 20 IC
slots. The 1-slot dept-wide shortage materializes as 2 UX Research
IC3 vacancies offset by 1 spillover at UX Research IC1.

These are accepted residuals. Downstream materialization (2e) treats
the cells as canonical; a Tableau or dbt-side note can flag them as
known spec artifacts when surfacing the level distribution.
"""

from __future__ import annotations

import random
import runpy
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

# Bumped only when 2c step 2's logic itself changes.
RANDOM_SEED = 20260425

CURRENT_DATE = date(2025, 3, 31)

CARLOS_EMPLOYEE_ID = "EMP-009"
CARLOS_NEW_SUBDEPT = "(VP Sales)"

LEVEL_ORDER = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "M3", "M4", "M5"]
LEVEL_INDEX = {level: i for i, level in enumerate(LEVEL_ORDER)}
IC_LEVELS = ["IC1", "IC2", "IC3", "IC4", "IC5"]

IN_SCOPE_DEPARTMENTS = [
    "Engineering", "Sales", "Customer Success", "Marketing",
    "Product", "G&A", "People",
]

# Per-archetype max promotions for active flex profiles. Values are the
# spec's tenure-based promotion budgets capped at the manager threshold.
ARCHETYPE_MAX_PROMOTIONS = {
    "High-flyer":         3,
    "Steady contributor": 1,
    "Internal mover":     1,
}

# Section 3 target grid (transcribed from spec Section 3). Includes
# leadership-only sub-departments such as (CTO), (CRO), (VP CS), etc.
SECTION_3_GRID: dict[tuple[str, str], dict[str, int]] = {
    ("Engineering", "Platform"):       {"IC1": 3, "IC2": 8, "IC3": 14, "IC4": 7, "IC5": 2, "M1": 4, "M2": 1, "M3": 0, "M4": 1, "M5": 0},
    ("Engineering", "AI/ML"):          {"IC1": 2, "IC2": 6, "IC3": 12, "IC4": 8, "IC5": 3, "M1": 3, "M2": 1, "M3": 0, "M4": 1, "M5": 0},
    ("Engineering", "Data"):           {"IC1": 2, "IC2": 5, "IC3": 10, "IC4": 6, "IC5": 1, "M1": 4, "M2": 1, "M3": 1, "M4": 0, "M5": 0},
    ("Engineering", "Infrastructure"): {"IC1": 3, "IC2": 7, "IC3": 11, "IC4": 5, "IC5": 1, "M1": 3, "M2": 1, "M3": 1, "M4": 0, "M5": 0},
    ("Engineering", "(CTO)"):          {"IC1": 0, "IC2": 0, "IC3": 0,  "IC4": 0, "IC5": 0, "M1": 0, "M2": 0, "M3": 0, "M4": 0, "M5": 1},
    ("Sales", "SDR"):                  {"IC1": 10,"IC2": 6, "IC3": 2,  "IC4": 0, "IC5": 0, "M1": 2, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("Sales", "Account Executive"):    {"IC1": 2, "IC2": 6, "IC3": 10, "IC4": 4, "IC5": 0, "M1": 3, "M2": 1, "M3": 1, "M4": 0, "M5": 0},
    ("Sales", "Account Management"):   {"IC1": 1, "IC2": 3, "IC3": 5,  "IC4": 2, "IC5": 0, "M1": 2, "M2": 1, "M3": 0, "M4": 0, "M5": 0},
    ("Sales", "Sales Engineering"):    {"IC1": 0, "IC2": 2, "IC3": 4,  "IC4": 2, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("Sales", "(CRO)"):                {"IC1": 0, "IC2": 0, "IC3": 0,  "IC4": 0, "IC5": 0, "M1": 0, "M2": 0, "M3": 0, "M4": 0, "M5": 1},
    ("Customer Success", "CSM"):       {"IC1": 2, "IC2": 4, "IC3": 6,  "IC4": 3, "IC5": 0, "M1": 2, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("Customer Success", "Support"):   {"IC1": 3, "IC2": 4, "IC3": 3,  "IC4": 1, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("Customer Success", "Implementation"): {"IC1": 1, "IC2": 2, "IC3": 3, "IC4": 1, "IC5": 0, "M1": 1, "M2": 1, "M3": 0, "M4": 0, "M5": 0},
    ("Customer Success", "(VP CS)"):   {"IC1": 0, "IC2": 0, "IC3": 0,  "IC4": 0, "IC5": 0, "M1": 0, "M2": 0, "M3": 0, "M4": 1, "M5": 0},
    ("Marketing", "Growth"):           {"IC1": 1, "IC2": 2, "IC3": 4,  "IC4": 2, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("Marketing", "Content"):          {"IC1": 1, "IC2": 2, "IC3": 3,  "IC4": 1, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("Marketing", "Product Marketing"):{"IC1": 0, "IC2": 1, "IC3": 2,  "IC4": 1, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("Marketing", "Brand"):            {"IC1": 0, "IC2": 1, "IC3": 2,  "IC4": 1, "IC5": 0, "M1": 0, "M2": 1, "M3": 0, "M4": 0, "M5": 0},
    ("Marketing", "(VP Mktg)"):        {"IC1": 0, "IC2": 0, "IC3": 0,  "IC4": 0, "IC5": 0, "M1": 0, "M2": 0, "M3": 0, "M4": 1, "M5": 0},
    ("Product", "Product Management"): {"IC1": 0, "IC2": 2, "IC3": 4,  "IC4": 3, "IC5": 0, "M1": 1, "M2": 0, "M3": 1, "M4": 0, "M5": 0},
    ("Product", "Design"):             {"IC1": 1, "IC2": 2, "IC3": 3,  "IC4": 1, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("Product", "UX Research"):        {"IC1": 0, "IC2": 1, "IC3": 2,  "IC4": 1, "IC5": 0, "M1": 1, "M2": 1, "M3": 0, "M4": 0, "M5": 0},
    ("Product", "(CPO)"):              {"IC1": 0, "IC2": 0, "IC3": 0,  "IC4": 0, "IC5": 0, "M1": 0, "M2": 0, "M3": 0, "M4": 0, "M5": 1},
    ("G&A", "Finance"):                {"IC1": 1, "IC2": 3, "IC3": 4,  "IC4": 2, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 1, "M5": 0},
    ("G&A", "Legal"):                  {"IC1": 0, "IC2": 1, "IC3": 2,  "IC4": 1, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("G&A", "IT"):                     {"IC1": 2, "IC2": 3, "IC3": 3,  "IC4": 1, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("G&A", "Facilities"):             {"IC1": 1, "IC2": 2, "IC3": 2,  "IC4": 0, "IC5": 0, "M1": 0, "M2": 1, "M3": 0, "M4": 0, "M5": 0},
    ("G&A", "(CFO)"):                  {"IC1": 0, "IC2": 0, "IC3": 0,  "IC4": 0, "IC5": 0, "M1": 0, "M2": 0, "M3": 0, "M4": 0, "M5": 1},
    ("People", "HRBPs"):               {"IC1": 0, "IC2": 1, "IC3": 2,  "IC4": 1, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("People", "Recruiting"):          {"IC1": 1, "IC2": 2, "IC3": 3,  "IC4": 1, "IC5": 0, "M1": 1, "M2": 0, "M3": 1, "M4": 0, "M5": 0},
    ("People", "People Ops"):          {"IC1": 1, "IC2": 1, "IC3": 2,  "IC4": 0, "IC5": 0, "M1": 1, "M2": 0, "M3": 1, "M4": 0, "M5": 0},
    ("People", "Total Rewards"):       {"IC1": 0, "IC2": 1, "IC3": 1,  "IC4": 1, "IC5": 0, "M1": 0, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("People", "L&D"):                 {"IC1": 1, "IC2": 1, "IC3": 2,  "IC4": 0, "IC5": 0, "M1": 1, "M2": 0, "M3": 0, "M4": 0, "M5": 0},
    ("People", "DEIB"):                {"IC1": 0, "IC2": 1, "IC3": 2,  "IC4": 0, "IC5": 0, "M1": 0, "M2": 1, "M3": 0, "M4": 0, "M5": 0},
    ("People", "(CPO)"):               {"IC1": 0, "IC2": 0, "IC3": 0,  "IC4": 0, "IC5": 0, "M1": 0, "M2": 0, "M3": 0, "M4": 0, "M5": 1},
}

# Carlos placeholder cell -- M4 in Sales/(VP Sales). Mirrors the way
# Lisa Park (VP CS) and Nina Okonkwo (VP Mktg) get dedicated sub-dept
# entries in Section 3.
CARLOS_VP_SALES_CELL = (
    ("Sales", CARLOS_NEW_SUBDEPT),
    {"IC1": 0, "IC2": 0, "IC3": 0, "IC4": 0, "IC5": 0,
     "M1": 0, "M2": 0, "M3": 0, "M4": 1, "M5": 0},
)

# Per-department uplift slots (1c added 8 active beyond Section 3 to
# hit 380; one of those is now Carlos in (VP Sales) so Sales drops
# from +2 to +1). Distributed to IC3 cells in dense sub-departments.
DEPT_UPLIFT_SLOTS: dict[str, list[tuple[str, str]]] = {
    "Engineering":      [("Platform", "IC3"), ("AI/ML", "IC3"), ("Infrastructure", "IC3")],
    "Sales":            [("Account Executive", "IC3")],
    "Customer Success": [("CSM", "IC3")],
    "G&A":              [("IT", "IC3")],
    "Marketing":        [("Growth", "IC3")],
    "Product":          [],
    "People":           [],
}

ASSIGNMENT_COLUMNS = [
    "employee_id",
    "archetype",
    "track",                       # active | terminated
    "department",
    "starting_sub_department",
    "final_sub_department",
    "starting_job_level",
    "final_job_level",
    "promotions_count",
    "subdept_changed",
]

EVENT_COLUMNS = [
    "employee_id",
    "event_type",                  # Promotion | Lateral Transfer
    "effective_date",
    "old_value",
    "new_value",
    "context",                     # provenance for 2e
]


# ---------------------------------------------------------------------------
# Loading + helpers
# ---------------------------------------------------------------------------

def _load_state() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run Stage 1 + Stage 2a in-process, return (profiles, designations)."""
    base = Path(__file__).parent
    stage1 = runpy.run_path(
        str(base / "01_generate_employee_profiles.py"), run_name="stage1"
    )
    stage2a = runpy.run_path(
        str(base / "02_designate_manager_layer.py"), run_name="stage2a"
    )
    return stage1["build_employee_profiles"](), stage2a["build_level_designations"]()


def _max_reachable_level(starting_level: str, archetype: str) -> str:
    """Return the highest IC level the profile can reach via promotions."""
    max_promo = ARCHETYPE_MAX_PROMOTIONS.get(archetype, 0)
    start_idx = LEVEL_INDEX[starting_level]
    cap_idx = LEVEL_INDEX["IC5"]
    return LEVEL_ORDER[min(start_idx + max_promo, cap_idx)]


def _spread_dates(hire_date: date, end_date: date, count: int) -> list[date]:
    """Spread `count` evenly-spaced dates strictly between hire and end."""
    if count <= 0:
        return []
    span_days = max(1, (end_date - hire_date).days)
    return [
        hire_date + timedelta(days=int(span_days * (i + 1) / (count + 1)))
        for i in range(count)
    ]


def _to_date(value) -> date:
    """Coerce a pd.Timestamp or datetime.date into a datetime.date."""
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


# ---------------------------------------------------------------------------
# Locked-profile resolution
# ---------------------------------------------------------------------------

def _pick_sub_department_for_level(
    department: str,
    preferred_sub_dept: str,
    level: str,
    remaining_capacity: dict[tuple[str, str, str], int],
) -> str:
    """Pick a sub-department within `department` that has capacity for `level`.

    Prefers `preferred_sub_dept` when it still has capacity (avoids creating
    a lateral transfer). Otherwise picks the sub-dept with the most
    remaining capacity for that level. Falls back to the preferred sub-dept
    if no slot exists -- which creates a delta but keeps the assignment
    coherent.
    """
    preferred_key = (department, preferred_sub_dept, level)
    if remaining_capacity.get(preferred_key, 0) > 0:
        return preferred_sub_dept

    candidates = [
        (sd, cap)
        for (d, sd, lvl), cap in remaining_capacity.items()
        if d == department and lvl == level and cap > 0
    ]
    if candidates:
        candidates.sort(key=lambda item: -item[1])
        return candidates[0][0]
    return preferred_sub_dept


def _make_transfer_event(
    profile_record: dict, old_sub_dept: str, new_sub_dept: str, context: str
) -> dict:
    """Lateral Transfer event at the midpoint of the profile's tenure."""
    hire_date = _to_date(profile_record["hire_date"])
    term_date = profile_record.get("termination_date")
    if term_date is not None and not pd.isna(term_date):
        end_date = _to_date(term_date)
    else:
        end_date = CURRENT_DATE
    tenure_days = max(1, (end_date - hire_date).days)
    effective = hire_date + timedelta(days=tenure_days // 2)
    return {
        "employee_id":    profile_record["employee_id"],
        "event_type":     "Lateral Transfer",
        "effective_date": effective,
        "old_value":      old_sub_dept,
        "new_value":      new_sub_dept,
        "context":        context,
    }


def _resolve_locked_assignments(
    profiles: pd.DataFrame,
    designations: pd.DataFrame,
    target_grid: dict[tuple[str, str, str], int],
) -> tuple[
    dict[str, tuple[str, str, str]],
    list[dict],
    dict[tuple[str, str, str], int],
]:
    """Assign (department, sub_department, level) for all level-locked active profiles.

    Process order, most constrained first:
      1. Defined leadership      -- explicit sub-dept from spec, with Carlos
                                    relocation to (VP Sales).
      2. M2 designations          -- distribute across sub-depts in each dept
                                    so the Section 3 M2 cells are filled.
      3. M1 designations          -- same, for M1 cells.
      4. Founder IC track         -- IC4 / IC5 distributed where Section 3
                                    has capacity.
      5. Manager Step-Back        -- IC4 placed in a sub-dept where the
                                    Section 3 IC4 cell has capacity.

    Each step picks a sub-department for the profile (preferring its
    Stage 1 sub-dept when capacity exists, else moving to a sub-dept
    that does). A sub-dept move is recorded as a Lateral Transfer event.

    Returns
    -------
    locked
        employee_id -> (department, sub_department, current_level)
    transfer_events
        list of Lateral Transfer event dicts
    remaining_capacity
        target_grid with locked-profile slots subtracted
    """
    designation_lookup = designations.set_index("employee_id").to_dict("index")
    locked: dict[str, tuple[str, str, str]] = {}
    transfer_events: list[dict] = []
    remaining = dict(target_grid)

    profile_records = profiles.to_dict("records")
    profiles_by_id = {p["employee_id"]: p for p in profile_records}

    # 1. Defined leadership.
    for profile in profile_records:
        if profile["employment_status"] != "Active" or not profile["is_leadership"]:
            continue
        emp_id = profile["employee_id"]
        sub_dept = profile["sub_department"]
        if emp_id == CARLOS_EMPLOYEE_ID:
            sub_dept = CARLOS_NEW_SUBDEPT
        level = profile["starting_job_level"]
        locked[emp_id] = (profile["department"], sub_dept, level)
        key = (profile["department"], sub_dept, level)
        remaining[key] = remaining.get(key, 0) - 1

    # 2 + 3. Manager designations (M2 first because they are scarcer per dept).
    for level in ("M2", "M1"):
        cohort = [
            row for row in designations.to_dict("records")
            if row["current_job_level"] == level
        ]
        # Process oldest-tenured first so the longest-tenured manager gets
        # to keep their Stage 1 sub-dept if it has a slot.
        cohort.sort(key=lambda row: _to_date(row["hire_date"]))
        for row in cohort:
            emp_id = row["employee_id"]
            if emp_id in locked:
                continue
            preferred = row["sub_department"]
            chosen = _pick_sub_department_for_level(
                row["department"], preferred, level, remaining
            )
            locked[emp_id] = (row["department"], chosen, level)
            key = (row["department"], chosen, level)
            remaining[key] = remaining.get(key, 0) - 1
            if chosen != preferred:
                transfer_events.append(
                    _make_transfer_event(
                        profiles_by_id[emp_id],
                        old_sub_dept=preferred,
                        new_sub_dept=chosen,
                        context="manager_subdept_rebalance",
                    )
                )

    # 4. Founder IC track (IC4 + IC5 from designations).
    founder_ic = [
        row for row in designations.to_dict("records")
        if row["current_job_level"] in {"IC4", "IC5"}
    ]
    founder_ic.sort(key=lambda row: _to_date(row["hire_date"]))
    for row in founder_ic:
        emp_id = row["employee_id"]
        if emp_id in locked:
            continue
        preferred = row["sub_department"]
        level = row["current_job_level"]
        chosen = _pick_sub_department_for_level(
            row["department"], preferred, level, remaining
        )
        locked[emp_id] = (row["department"], chosen, level)
        key = (row["department"], chosen, level)
        remaining[key] = remaining.get(key, 0) - 1
        if chosen != preferred:
            transfer_events.append(
                _make_transfer_event(
                    profiles_by_id[emp_id],
                    old_sub_dept=preferred,
                    new_sub_dept=chosen,
                    context="founder_ic_subdept_rebalance",
                )
            )

    # 5. Manager Step-Back (IC4 lock).
    for profile in profile_records:
        if (
            profile["employment_status"] != "Active"
            or profile["archetype"] != "Manager step-back"
        ):
            continue
        emp_id = profile["employee_id"]
        if emp_id in locked:
            continue
        preferred = profile["sub_department"]
        chosen = _pick_sub_department_for_level(
            profile["department"], preferred, "IC4", remaining
        )
        locked[emp_id] = (profile["department"], chosen, "IC4")
        key = (profile["department"], chosen, "IC4")
        remaining[key] = remaining.get(key, 0) - 1
        if chosen != preferred:
            transfer_events.append(
                _make_transfer_event(
                    profile,
                    old_sub_dept=preferred,
                    new_sub_dept=chosen,
                    context="step_back_subdept_rebalance",
                )
            )

    return locked, transfer_events, remaining


# ---------------------------------------------------------------------------
# Target grid
# ---------------------------------------------------------------------------

def _build_target_grid_with_uplift() -> dict[tuple[str, str, str], int]:
    """Return (dept, sub_dept, level) -> capacity. Section 3 + (VP Sales) + uplift."""
    target: dict[tuple[str, str, str], int] = {}
    for (dept, sd), levels in SECTION_3_GRID.items():
        for level, count in levels.items():
            target[(dept, sd, level)] = count
    (carlos_key, carlos_levels) = CARLOS_VP_SALES_CELL
    for level, count in carlos_levels.items():
        target[(carlos_key[0], carlos_key[1], level)] = count

    for dept, slots in DEPT_UPLIFT_SLOTS.items():
        for sd, level in slots:
            target[(dept, sd, level)] = target.get((dept, sd, level), 0) + 1

    return target


def _consume_locked(
    target_grid: dict[tuple[str, str, str], int],
    locked: dict[str, tuple[str, str, str]],
) -> dict[tuple[str, str, str], int]:
    """Subtract locked profile counts from target grid; return remaining slots."""
    remaining = dict(target_grid)
    for _, (dept, sd, level) in locked.items():
        key = (dept, sd, level)
        remaining[key] = remaining.get(key, 0) - 1
    return remaining


# ---------------------------------------------------------------------------
# Greedy flex assignment
# ---------------------------------------------------------------------------

def _greedy_assign_flex(
    profiles: pd.DataFrame,
    locked: dict[str, tuple[str, str, str]],
    remaining_targets: dict[tuple[str, str, str], int],
) -> tuple[
    dict[str, tuple[str, str, str]],
    list[dict],
    list[dict],
]:
    """Greedy fill of remaining IC slots using flex profiles.

    Per department:
      - Slot list is sorted by level descending (IC5 first), then sub-dept
        name, so the most senior IC seats find their best-fit profile
        before the cheaper IC1 / IC2 seats.
      - For each slot, pick the eligible profile that minimizes the tuple
        (sub_dept_changed, promotions_needed, -tenure_days) -- prefer
        same sub-dept, then minimum promotions, then longer tenure as
        tiebreaker.

    Returns (assignments, promotion_events, subdept_change_events).
    """
    flex_profiles = profiles[
        (profiles["employment_status"] == "Active")
        & (~profiles["employee_id"].isin(locked))
    ].copy()
    flex_profiles["tenure_days"] = (
        pd.Timestamp(CURRENT_DATE) - flex_profiles["hire_date"]
    ).dt.days

    flex_assignments: dict[str, tuple[str, str, str]] = {}
    promotion_events: list[dict] = []
    subdept_change_events: list[dict] = []

    for dept, dept_flex in flex_profiles.groupby("department"):
        dept_slots = [
            (sd, level, count)
            for (d, sd, level), count in remaining_targets.items()
            if d == dept and count > 0
        ]
        slot_queue: list[tuple[str, str]] = []
        for sd, level, count in dept_slots:
            for _ in range(count):
                slot_queue.append((sd, level))
        # Highest level first; alphabetical sub_dept tiebreaker.
        slot_queue.sort(key=lambda item: (-LEVEL_INDEX[item[1]], item[0]))

        unassigned = dept_flex.to_dict("records")

        for slot_sd, slot_level in slot_queue:
            slot_level_idx = LEVEL_INDEX[slot_level]
            best_idx = None
            best_key = None

            for idx, candidate in enumerate(unassigned):
                starting_idx = LEVEL_INDEX[candidate["starting_job_level"]]
                # Never demote: candidate's starting level must be at or
                # below the slot level. Otherwise the negative
                # `promos_needed` would beat zero-promotion candidates in
                # the sort and produce IC4 -> IC3 / IC2 -> IC1 demotions.
                if starting_idx > slot_level_idx:
                    continue
                max_level = _max_reachable_level(
                    candidate["starting_job_level"], candidate["archetype"]
                )
                if LEVEL_INDEX[max_level] < slot_level_idx:
                    continue
                same_sd = 0 if candidate["sub_department"] == slot_sd else 1
                promos_needed = slot_level_idx - starting_idx
                key = (same_sd, promos_needed, -candidate["tenure_days"])
                if best_key is None or key < best_key:
                    best_key = key
                    best_idx = idx

            if best_idx is None:
                continue

            chosen = unassigned.pop(best_idx)
            emp_id = chosen["employee_id"]
            flex_assignments[emp_id] = (dept, slot_sd, slot_level)

            promotions_needed = (
                slot_level_idx - LEVEL_INDEX[chosen["starting_job_level"]]
            )
            if promotions_needed > 0:
                hire_date_d = _to_date(chosen["hire_date"])
                end_date_d = (
                    _to_date(chosen["termination_date"])
                    if chosen["employment_status"] != "Active" else CURRENT_DATE
                )
                promo_dates = _spread_dates(hire_date_d, end_date_d, promotions_needed)
                level_idx = LEVEL_INDEX[chosen["starting_job_level"]]
                for promo_date in promo_dates:
                    promotion_events.append({
                        "employee_id":    emp_id,
                        "event_type":     "Promotion",
                        "effective_date": promo_date,
                        "old_value":      LEVEL_ORDER[level_idx],
                        "new_value":      LEVEL_ORDER[level_idx + 1],
                        "context":        "flex_alignment",
                    })
                    level_idx += 1

            if chosen["sub_department"] != slot_sd:
                hire_date_d = _to_date(chosen["hire_date"])
                end_date_d = (
                    _to_date(chosen["termination_date"])
                    if chosen["employment_status"] != "Active" else CURRENT_DATE
                )
                tenure_days = max(1, (end_date_d - hire_date_d).days)
                # Place lateral transfer at midpoint of tenure.
                effective = hire_date_d + timedelta(days=tenure_days // 2)
                subdept_change_events.append({
                    "employee_id":    emp_id,
                    "event_type":     "Lateral Transfer",
                    "effective_date": effective,
                    "old_value":      chosen["sub_department"],
                    "new_value":      slot_sd,
                    "context":        "subdept_rebalance",
                })

        # Spillover: any unassigned active flex profile keeps its starting state.
        for leftover in unassigned:
            flex_assignments[leftover["employee_id"]] = (
                dept, leftover["sub_department"], leftover["starting_job_level"],
            )

    return flex_assignments, promotion_events, subdept_change_events


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build_aligned_grid() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Compute the post-2c level/sub-dept assignments + tracking events."""
    profiles, designations = _load_state()
    target_grid = _build_target_grid_with_uplift()
    locked, locked_transfer_events, remaining = _resolve_locked_assignments(
        profiles, designations, target_grid
    )
    flex_assignments, promotion_events, flex_transfer_events = _greedy_assign_flex(
        profiles, locked, remaining
    )
    subdept_change_events = locked_transfer_events + flex_transfer_events

    final_assignments: dict[str, tuple[str, str, str]] = {}
    final_assignments.update(locked)
    final_assignments.update(flex_assignments)

    rows = []
    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        if profile["employment_status"] != "Active":
            rows.append({
                "employee_id":             emp_id,
                "archetype":               profile["archetype"],
                "track":                   "terminated",
                "department":              profile["department"],
                "starting_sub_department": profile["sub_department"],
                "final_sub_department":    profile["sub_department"],
                "starting_job_level":      profile["starting_job_level"],
                "final_job_level":         profile["starting_job_level"],
                "promotions_count":        0,
                "subdept_changed":         False,
            })
            continue

        if emp_id not in final_assignments:
            continue

        dept, final_sd, final_level = final_assignments[emp_id]
        starting_idx = LEVEL_INDEX[profile["starting_job_level"]]
        final_idx = LEVEL_INDEX.get(final_level, starting_idx)
        promo_count = max(0, final_idx - starting_idx)
        rows.append({
            "employee_id":             emp_id,
            "archetype":               profile["archetype"],
            "track":                   "active",
            "department":              dept,
            "starting_sub_department": profile["sub_department"],
            "final_sub_department":    final_sd,
            "starting_job_level":      profile["starting_job_level"],
            "final_job_level":         final_level,
            "promotions_count":        promo_count,
            "subdept_changed":         profile["sub_department"] != final_sd,
        })

    assignments_df = pd.DataFrame(rows, columns=ASSIGNMENT_COLUMNS)
    promotion_events_df = pd.DataFrame(promotion_events, columns=EVENT_COLUMNS)
    subdept_change_events_df = pd.DataFrame(subdept_change_events, columns=EVENT_COLUMNS)
    return assignments_df, promotion_events_df, subdept_change_events_df


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_alignment_summary(
    assignments_df: pd.DataFrame,
    promotion_events_df: pd.DataFrame,
    subdept_change_events_df: pd.DataFrame,
) -> None:
    """Print updated grid + delta vs Section 3 + event totals."""
    active = assignments_df[assignments_df["track"] == "active"]
    in_scope = active[active["department"].isin(IN_SCOPE_DEPARTMENTS)]

    current_grid = (
        in_scope.groupby(["department", "final_sub_department", "final_job_level"])
        .size()
        .unstack(fill_value=0)
    )
    for level in LEVEL_ORDER:
        if level not in current_grid.columns:
            current_grid[level] = 0
    current_grid = current_grid[LEVEL_ORDER].astype(int)
    current_grid.index = current_grid.index.set_names(["department", "sub_department"])

    # Build target grid that matches what the algorithm targeted: Section 3
    # + (VP Sales) extension + the per-dept uplift slots. Comparing against
    # this target keeps the audit focused on real misalignments rather than
    # surfacing the uplift cells as deltas.
    target_capacity = _build_target_grid_with_uplift()
    target_records: dict[tuple[str, str], dict[str, int]] = defaultdict(
        lambda: {level: 0 for level in LEVEL_ORDER}
    )
    for (dept, sd, level), count in target_capacity.items():
        target_records[(dept, sd)][level] = count
    target_grid = (
        pd.DataFrame([
            {"department": dept, "sub_department": sd, **levels}
            for (dept, sd), levels in target_records.items()
        ])
        .set_index(["department", "sub_department"])[LEVEL_ORDER]
        .astype(int)
    )

    all_keys = sorted(set(current_grid.index) | set(target_grid.index))
    current_grid = current_grid.reindex(all_keys, fill_value=0)
    target_grid = target_grid.reindex(all_keys, fill_value=0)
    delta_grid = current_grid - target_grid

    print("\n=== Per-department totals (post-alignment) ===")
    summary = pd.DataFrame({
        "current": current_grid.sum(axis=1).groupby("department").sum(),
        "target":  target_grid.sum(axis=1).groupby("department").sum(),
    })
    summary["delta"] = summary["current"] - summary["target"]
    summary.loc["TOTAL"] = summary.sum(numeric_only=True)
    print(summary.to_string())

    print("\n=== Delta cells (current - target) where delta != 0 ===")
    delta_grid.columns.name = "level"
    delta_long = (
        delta_grid.stack().reset_index().rename(columns={0: "delta"})
    )
    delta_long["delta"] = delta_long["delta"].astype(int)
    nonzero = delta_long[delta_long["delta"] != 0].copy()
    nonzero["current"] = nonzero.apply(
        lambda r: int(current_grid.loc[(r["department"], r["sub_department"]), r["level"]]),
        axis=1,
    )
    nonzero["target"] = nonzero.apply(
        lambda r: int(target_grid.loc[(r["department"], r["sub_department"]), r["level"]]),
        axis=1,
    )
    nonzero = (
        nonzero[["department", "sub_department", "level", "current", "target", "delta"]]
        .sort_values(["department", "sub_department", "level"])
    )
    if nonzero.empty:
        print("  (none -- grid matches target exactly)")
    else:
        print(nonzero.to_string(index=False))

    surplus = int(nonzero[nonzero["delta"] > 0]["delta"].sum()) if not nonzero.empty else 0
    shortage = int(-nonzero[nonzero["delta"] < 0]["delta"].sum()) if not nonzero.empty else 0
    print(
        f"\n  cells with non-zero delta: {len(nonzero)} of {len(delta_long)}"
    )
    print(f"  total surplus  (current > target): {surplus}")
    print(f"  total shortage (current < target): {shortage}")

    print("\n=== Delta by level (collapsed across departments) ===")
    print(delta_grid.sum().astype(int).rename("delta_total").to_string())

    print("\n=== Promotion events ===")
    print(f"  total promotion events: {len(promotion_events_df)}")
    if not promotion_events_df.empty:
        merged = promotion_events_df.merge(
            assignments_df[["employee_id", "archetype"]], on="employee_id"
        )
        print("\n  by archetype:")
        print(merged.groupby("archetype").size().to_string())
        print("\n  by old_value -> new_value:")
        print(
            merged.groupby(["old_value", "new_value"]).size().to_string()
        )

    print("\n=== Sub-department transfer events ===")
    print(f"  total transfer events: {len(subdept_change_events_df)}")

    print("\n=== Active profile change summary ===")
    n_active = len(active)
    print(f"  active profiles:         {n_active}")
    print(f"  promoted (>= 1 promo):   {int((active['promotions_count'] > 0).sum())}")
    print(f"  sub-department changed:  {int(active['subdept_changed'].sum())}")
    print(f"  Carlos relocated:        Sales/Account Executive -> Sales/{CARLOS_NEW_SUBDEPT}")


if __name__ == "__main__":
    assignments_df, promotion_events_df, subdept_change_events_df = build_aligned_grid()
    print_alignment_summary(
        assignments_df, promotion_events_df, subdept_change_events_df
    )
