"""
Substage 2b: Manager hierarchy resolution.

Purpose
-------
Assign manager_id for every one of the 604 employee profiles, building
the complete reporting tree. CEO Maya Chen has manager_id = None (the
Board); every other profile reports to another employee whose
department, level, and timing satisfy the spec's reporting rules.

Inputs
------
- Stage 1: `build_employee_profiles()` (604 rows).
- Stage 2a: `build_level_designations()` (49 manager designations + 10
  founder IC designations).

Outputs
-------
- `build_manager_hierarchy()` returns a DataFrame keyed by
  employee_id with the columns listed in `ASSIGNMENT_COLUMNS`. Each
  row carries the assigned manager_id, the manager's level and
  department, and the assignment pathway used (for review).

Pipeline placement
------------------
Substage 2b. Runs after 2a. Output feeds 2c (level alignment) and 2e
(raw_employees / raw_job_history materialization).

Reporting rules
---------------
Hardcoded leadership chain (from Section 2):

    EMP-001 Maya Chen        CEO        -> None  (Board)
    EMP-002 David Okafor     CTO   M5   -> Maya
    EMP-003 Marcus Lee       CRO   M5   -> Maya
    EMP-004 Aisha Patel      CFO   M5   -> Maya
    EMP-005 James Wallace    CPO   M5   -> Maya
    EMP-006 Rachel Torres    CPO People M5 -> Maya
    EMP-007 Kevin Zhao       VP Eng Platform M4 -> David
    EMP-008 Amara Johnson    VP Eng AI/ML   M4 -> David
    EMP-009 Carlos Mendez    VP Sales       M4 -> Marcus
    EMP-010 Lisa Park        VP Customer Success M4 -> Marcus  (CS rolls under CRO)
    EMP-011 Nina Okonkwo     VP Marketing   M4 -> Maya         (no CMO)
    EMP-012 Raj Gupta        VP Finance     M4 -> Aisha
    EMP-013 Sarah Kim        Director Eng Data       M3 -> David
    EMP-014 Jordan Brooks    Director Eng Infra      M3 -> David
    EMP-015 Michelle Torres  Director Recruiting     M3 -> Rachel
    EMP-016 Derek Washington Director People Ops     M3 -> Rachel
    EMP-017 Hannah Lee       Director Product        M3 -> James
    EMP-018 Andre Williams   Director Sales          M3 -> Carlos
    EMP-019 Priya Sharma     Co-Founder/CPO (departed) M3 -> Maya

Below the leadership block:
    M2  -> first valid candidate among same-sub-dept M3, same-dept M3,
           same-sub-dept M4, same-dept M4, then M5.
    M1  -> same-sub-dept M2, same-dept M2, then M3 / M4 / M5 in same
           dept (mirroring the level cascade).
    ICs -> same-sub-dept M1, same-dept M1, then M2 / M3+ as fallback.

Timing constraint:
    Manager.hire_date < Reportee.hire_date AND, depending on
    employment status:
        active reportee     -> manager must currently be active
        terminated reportee -> manager must have been active at
                                reportee's termination_date
    This makes Priya Sharma (terminated 2023-06-30) eligible only as a
    manager for terminated profiles whose termination_date is on or
    before 2023-06-30.

Span of control:
    Within each (level, tier) candidate set, the candidate with the
    fewest existing reports is chosen first. Section 2 cites ~1
    manager per 7-8 ICs in Engineering / CS / Sales; the actual span
    landings are reported in the summary so deviations can be reviewed
    before 2c rebalances the level grid.
"""

from __future__ import annotations

import random
import runpy
from collections import defaultdict
from datetime import date
from pathlib import Path

import pandas as pd

# Bumped only if 2b's selection logic changes. Stage 1 / 2a seeds govern
# the upstream profile + designation distributions.
RANDOM_SEED = 20260425

# Reference date for "active" status -- the spec's current-state cutoff.
CURRENT_DATE = date(2025, 3, 31)

# Hardcoded leadership reporting chain per Section 2 of the spec.
LEADERSHIP_MANAGER_IDS: dict[str, str | None] = {
    "EMP-001": None,
    "EMP-002": "EMP-001",
    "EMP-003": "EMP-001",
    "EMP-004": "EMP-001",
    "EMP-005": "EMP-001",
    "EMP-006": "EMP-001",
    "EMP-007": "EMP-002",
    "EMP-008": "EMP-002",
    "EMP-009": "EMP-003",
    "EMP-010": "EMP-003",
    "EMP-011": "EMP-001",
    "EMP-012": "EMP-004",
    "EMP-013": "EMP-002",
    "EMP-014": "EMP-002",
    "EMP-015": "EMP-006",
    "EMP-016": "EMP-006",
    "EMP-017": "EMP-005",
    "EMP-018": "EMP-009",
    "EMP-019": "EMP-001",
}

# Acceptable manager levels for each reporter level (priority order).
ACCEPTABLE_MANAGER_LEVELS: dict[str, tuple[str, ...]] = {
    "M4": ("M5",),
    "M3": ("M4", "M5"),
    "M2": ("M3", "M4", "M5"),
    "M1": ("M2", "M3", "M4", "M5"),
    "IC5": ("M1", "M2", "M3", "M4", "M5"),
    "IC4": ("M1", "M2", "M3", "M4", "M5"),
    "IC3": ("M1", "M2", "M3", "M4", "M5"),
    "IC2": ("M1", "M2", "M3", "M4", "M5"),
    "IC1": ("M1", "M2", "M3", "M4", "M5"),
}

# Levels processed top-down so each tier's parents already exist when we
# reach the children. Leadership is handled before this loop runs.
PROCESSING_ORDER: tuple[str, ...] = (
    "M2", "M1", "IC5", "IC4", "IC3", "IC2", "IC1",
)

ASSIGNMENT_COLUMNS = [
    "employee_id",
    "department",
    "sub_department",
    "current_job_level",
    "manager_id",
    "manager_level",
    "manager_department",
    "manager_sub_department",
    "manager_assignment_pathway",
]


# ---------------------------------------------------------------------------
# Loading upstream stages
# ---------------------------------------------------------------------------

def _load_employee_profiles() -> pd.DataFrame:
    """Run Stage 1 in-process and return its profile DataFrame."""
    stage1_path = Path(__file__).parent / "01_generate_employee_profiles.py"
    namespace = runpy.run_path(str(stage1_path), run_name="stage1")
    return namespace["build_employee_profiles"]()


def _load_level_designations() -> pd.DataFrame:
    """Run Stage 2a in-process and return its designation DataFrame."""
    stage2a_path = Path(__file__).parent / "02_designate_manager_layer.py"
    namespace = runpy.run_path(str(stage2a_path), run_name="stage2a")
    return namespace["build_level_designations"]()


# ---------------------------------------------------------------------------
# Level resolution
# ---------------------------------------------------------------------------

def _build_current_level_lookup(
    profiles: pd.DataFrame, designations: pd.DataFrame
) -> dict[str, str]:
    """Return employee_id -> current_job_level.

    Resolution order: leadership (from Section 2), then 2a designations
    (manager M1/M2 + founder IC4/IC5), then Manager Step-Back override
    (current = IC4 by archetype rule), then everyone else uses Stage 1
    starting_job_level as a placeholder. The placeholder is refined in
    2c when the active sub-dept x level grid is aligned to Section 3.
    """
    level_lookup: dict[str, str] = {}
    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        if profile["is_leadership"]:
            level_lookup[emp_id] = profile["starting_job_level"]
        elif profile["archetype"] == "Manager step-back":
            level_lookup[emp_id] = "IC4"
        else:
            level_lookup[emp_id] = profile["starting_job_level"]
    for _, designation in designations.iterrows():
        level_lookup[designation["employee_id"]] = designation["current_job_level"]
    return level_lookup


# ---------------------------------------------------------------------------
# Manager validity & search
# ---------------------------------------------------------------------------

def _is_valid_manager(
    manager: pd.Series, reportee: pd.Series
) -> bool:
    """Check whether `manager` is a valid current/last manager for `reportee`.

    The rule encodes raw_employees semantics: manager_id is the
    employee's *current* manager (or last manager, for terminated
    employees), not the one they had at hire time. So the constraint
    is that the manager exists and is active at the reportee's
    reference date:

        active reportee     -> ref_date = 2025-03-31; manager hired
                                by then and currently active.
        terminated reportee -> ref_date = reportee.termination_date;
                                manager hired by then and active on
                                that date (still active OR terminated
                                on/after the reportee).

    A manager hired AFTER the reportee is allowed, because real orgs
    routinely add a new boss above an existing employee. The original
    manager-at-hire is a job_history concern (later substage), not a
    raw_employees one.
    """
    if reportee["employment_status"] == "Active":
        reference_date = pd.Timestamp(CURRENT_DATE)
    else:
        reference_date = reportee["termination_date"]

    if manager["hire_date"] > reference_date:
        return False
    if manager["employment_status"] == "Active":
        return True
    return manager["termination_date"] >= reference_date


def _build_managers_index(
    profiles: pd.DataFrame,
) -> tuple[
    dict[tuple[str, str, str], list[pd.Series]],
    dict[tuple[str, str], list[pd.Series]],
]:
    """Index manager-eligible profiles by (dept, sub_dept, level) and (dept, level).

    Eligibility = current level starts with 'M'. Both indices share the
    same row references so updates are not needed when we mutate the
    span counters elsewhere.
    """
    by_dept_sub_level: dict[tuple[str, str, str], list[pd.Series]] = defaultdict(list)
    by_dept_level: dict[tuple[str, str], list[pd.Series]] = defaultdict(list)
    for _, profile in profiles.iterrows():
        level = profile["current_job_level"]
        if not isinstance(level, str) or not level.startswith("M"):
            continue
        by_dept_sub_level[
            (profile["department"], profile["sub_department"], level)
        ].append(profile)
        by_dept_level[(profile["department"], level)].append(profile)
    return by_dept_sub_level, by_dept_level


def _find_best_manager(
    reportee: pd.Series,
    by_dept_sub_level: dict[tuple[str, str, str], list[pd.Series]],
    by_dept_level: dict[tuple[str, str], list[pd.Series]],
    span_count: dict[str, int],
    all_managers: list[pd.Series],
) -> tuple[str, str]:
    """Resolve (manager_id, pathway_label) for one reportee.

    Cascade: for each acceptable manager level in priority order, try
    same sub-dept then same department. Returns first tier with a valid
    candidate, picking the candidate with the fewest current reports.
    Raises ValueError if no valid manager exists across all tiers and
    cross-dept fallback.
    """
    reporter_level = reportee["current_job_level"]
    target_levels = ACCEPTABLE_MANAGER_LEVELS.get(reporter_level, ())
    if not target_levels:
        raise ValueError(
            f"No acceptable manager levels configured for {reporter_level}"
        )

    for manager_level in target_levels:
        for tier_label, candidates in (
            ("same_sub_dept", by_dept_sub_level.get(
                (reportee["department"], reportee["sub_department"], manager_level), []
            )),
            ("same_dept", by_dept_level.get(
                (reportee["department"], manager_level), []
            )),
        ):
            valid = [
                c for c in candidates
                if c["employee_id"] != reportee["employee_id"]
                and _is_valid_manager(c, reportee)
            ]
            if not valid:
                continue
            valid.sort(key=lambda c: span_count[c["employee_id"]])
            chosen = valid[0]
            return (
                chosen["employee_id"],
                f"to_{manager_level}_{tier_label}",
            )

    # Cross-dept fallback (rare; Marketing/Product/People sub-depts
    # without local M3 sometimes need an M5 in another department).
    for manager_level in target_levels:
        candidates = [
            m for m in all_managers
            if m["current_job_level"] == manager_level
            and m["employee_id"] != reportee["employee_id"]
            and _is_valid_manager(m, reportee)
        ]
        if not candidates:
            continue
        candidates.sort(key=lambda c: span_count[c["employee_id"]])
        chosen = candidates[0]
        return (
            chosen["employee_id"],
            f"cross_dept_to_{manager_level}",
        )

    raise ValueError(
        f"No valid manager for {reportee['employee_id']} "
        f"({reportee['department']}/{reportee['sub_department']}, "
        f"{reporter_level})"
    )


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build_manager_hierarchy() -> pd.DataFrame:
    """Resolve manager_id for all 604 profiles.

    Pure function: deterministic given upstream Stage 1 / 2a output.
    """
    profiles = _load_employee_profiles()
    designations = _load_level_designations()
    level_lookup = _build_current_level_lookup(profiles, designations)

    profiles = profiles.copy()
    profiles["current_job_level"] = profiles["employee_id"].map(level_lookup)

    by_dept_sub_level, by_dept_level = _build_managers_index(profiles)
    all_managers = [
        p for _, p in profiles.iterrows()
        if isinstance(p["current_job_level"], str)
        and p["current_job_level"].startswith("M")
    ]

    manager_id_map: dict[str, str | None] = {}
    pathway_map: dict[str, str] = {}
    span_count: dict[str, int] = defaultdict(int)

    # 1. Leadership chain: hardcoded.
    for emp_id, manager_id in LEADERSHIP_MANAGER_IDS.items():
        manager_id_map[emp_id] = manager_id
        pathway_map[emp_id] = "leadership_chain"
        if manager_id is not None:
            span_count[manager_id] += 1

    # 2-3-4. Process remaining profiles in level order, top-down. Within
    # a level we sort by hire_date so longer-tenured profiles get first
    # pick of the lowest-span manager (slightly stabilizes round-robin).
    for current_level in PROCESSING_ORDER:
        cohort = (
            profiles[profiles["current_job_level"] == current_level]
            .sort_values("hire_date")
        )
        cohort = cohort[~cohort["employee_id"].isin(manager_id_map)]
        for _, reportee in cohort.iterrows():
            manager_id, pathway = _find_best_manager(
                reportee=reportee,
                by_dept_sub_level=by_dept_sub_level,
                by_dept_level=by_dept_level,
                span_count=span_count,
                all_managers=all_managers,
            )
            manager_id_map[reportee["employee_id"]] = manager_id
            pathway_map[reportee["employee_id"]] = pathway
            span_count[manager_id] += 1

    # Coverage check: every profile must have an entry (Maya's value is None).
    missing = set(profiles["employee_id"]) - set(manager_id_map)
    if missing:
        raise RuntimeError(
            f"Manager assignment missed {len(missing)} profiles: "
            f"{sorted(missing)[:5]}..."
        )

    # Build output DataFrame with manager metadata.
    profile_lookup = profiles.set_index("employee_id")
    rows: list[dict] = []
    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        manager_id = manager_id_map[emp_id]
        if manager_id is None:
            manager_level = None
            manager_dept = None
            manager_sub = None
        else:
            manager = profile_lookup.loc[manager_id]
            manager_level = manager["current_job_level"]
            manager_dept = manager["department"]
            manager_sub = manager["sub_department"]
        rows.append({
            "employee_id":                emp_id,
            "department":                 profile["department"],
            "sub_department":             profile["sub_department"],
            "current_job_level":          profile["current_job_level"],
            "manager_id":                 manager_id,
            "manager_level":              manager_level,
            "manager_department":         manager_dept,
            "manager_sub_department":     manager_sub,
            "manager_assignment_pathway": pathway_map[emp_id],
        })

    return pd.DataFrame(rows, columns=ASSIGNMENT_COLUMNS)


# ---------------------------------------------------------------------------
# Reporting & validation
# ---------------------------------------------------------------------------

def print_hierarchy_summary(assignments: pd.DataFrame) -> None:
    """Print review tables for the assigned reporting tree."""
    total = len(assignments)
    has_manager = assignments["manager_id"].notna().sum()
    no_manager = assignments["manager_id"].isna().sum()
    print(f"\nTotal profiles assigned: {total}")
    print(f"  with manager_id:      {has_manager}")
    print(f"  null manager_id:      {no_manager}  (expected 1: CEO)")

    print("\n--- Assignment pathway distribution ---")
    print(assignments["manager_assignment_pathway"].value_counts().to_string())

    print("\n--- Reporter level x manager level cross-tab ---")
    crosstab = pd.crosstab(
        assignments["current_job_level"].fillna("NULL"),
        assignments["manager_level"].fillna("NULL"),
    )
    print(crosstab.to_string())

    print("\n--- Cross-department reports (expected only at leadership chain) ---")
    cross_dept = assignments[
        (assignments["manager_id"].notna())
        & (assignments["department"] != assignments["manager_department"])
    ]
    print(f"  count: {len(cross_dept)}")
    if len(cross_dept) > 0:
        print(
            cross_dept[["employee_id", "department", "manager_id", "manager_department",
                        "current_job_level", "manager_level"]]
            .to_string(index=False)
        )

    print("\n--- Span of control (per manager, all reports) ---")
    span = (
        assignments[assignments["manager_id"].notna()]
        .groupby("manager_id")
        .size()
        .rename("reports")
    )
    span_with_level = span.to_frame().join(
        assignments.set_index("employee_id")[["current_job_level", "department"]],
        how="left",
    )
    print(
        span_with_level
        .groupby("current_job_level")["reports"]
        .agg(["count", "min", "median", "max", "mean"])
        .round(1)
        .to_string()
    )

    print("\n--- M1 span of control by department ---")
    m1_span = span_with_level[span_with_level["current_job_level"] == "M1"]
    print(
        m1_span.groupby("department")["reports"]
        .agg(["count", "min", "median", "max", "mean"])
        .round(1)
        .to_string()
    )

    print("\n--- Top 10 highest-span managers ---")
    top10 = span_with_level.nlargest(10, "reports")
    print(top10.to_string())

    print("\n--- Managers with zero reports (under-utilized) ---")
    all_managers = assignments[
        assignments["current_job_level"].fillna("").str.startswith("M")
    ]["employee_id"]
    zero_span = set(all_managers) - set(span.index)
    print(f"  count: {len(zero_span)}")
    if zero_span:
        zero_df = (
            assignments.set_index("employee_id")
            .loc[list(zero_span), ["current_job_level", "department", "sub_department"]]
        )
        print(zero_df.to_string())

    print("\n--- Validation: timing + dept-match invariants ---")
    print(_validate_assignments(assignments))


def _validate_assignments(assignments: pd.DataFrame) -> str:
    """Verify invariants hold across the assignment table."""
    findings = []

    null_count = assignments["manager_id"].isna().sum()
    if null_count != 1:
        findings.append(
            f"FAIL: expected exactly 1 null manager_id (Maya), got {null_count}"
        )
    elif assignments[assignments["manager_id"].isna()].iloc[0]["employee_id"] != "EMP-001":
        findings.append(
            "FAIL: null manager_id is not CEO (EMP-001)"
        )
    else:
        findings.append("OK: exactly 1 null manager_id (Maya)")

    leadership_ids = set(LEADERSHIP_MANAGER_IDS)
    cross_dept = assignments[
        (assignments["manager_id"].notna())
        & (assignments["department"] != assignments["manager_department"])
    ]
    unexpected_cross = cross_dept[~cross_dept["employee_id"].isin(leadership_ids)]
    if len(unexpected_cross) > 0:
        findings.append(
            f"FAIL: {len(unexpected_cross)} non-leadership profiles have a "
            f"cross-department manager"
        )
    else:
        findings.append(
            f"OK: all {len(cross_dept)} cross-department reports are within "
            f"the leadership chain"
        )

    return "\n  " + "\n  ".join(findings)


if __name__ == "__main__":
    assignments = build_manager_hierarchy()
    print_hierarchy_summary(assignments)
