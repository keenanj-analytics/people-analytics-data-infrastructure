"""
Substage 2e: raw_employees materialization + raw_job_history Hire rows.

Combines all Stage 2 outputs into:
  1. raw_employees DataFrame (604 rows, 18 fields per data dictionary)
  2. raw_job_history Hire rows (604 rows; one per employee)

Inputs
------
  - Stage 1: build_employee_profiles()
  - Stage 2a: build_level_designations()
  - Stage 2c: build_aligned_grid()
  - Stage 2d: build_employee_demographics()

Outputs
-------
  build_raw_employees()              -> 604-row DataFrame, all 18 fields
  build_raw_job_history_hire_rows()  -> 604-row DataFrame, raw_job_history Hire rows

Both are returned as DataFrames; CSV writes are deferred until the
full six-table coherence pass runs.

Design notes
------------
manager_id resolution runs twice on different reference dates:
  - raw_employees uses the *current* manager (active reportees: today;
    terminated reportees: their termination_date).
  - raw_job_history Hire rows use the *at-hire* manager (whoever was a
    valid manager when the reportee was hired).

The cascade is the same: same sub-dept first, then same dept; manager
level priority by reporter level (M1 -> M2/M3+, IC -> M1+, etc.).
Leadership reporting follows the hardcoded Section 2 chain in both
resolutions. CS rolls into CRO, Marketing rolls into CEO.

Hire rows reflect each employee's at-hire state, not current:
  - new_job_level    : effective_starting_level (M1 for external direct
                       M1 hires from 2a; IC3/IC4 for internal promotions
                       from 2a; IC3 for Manager Step-Back; otherwise
                       Stage 1 starting_job_level).
  - new_department   : Stage 1 department (no department transfers in
                       this dataset).
  - new_sub_department: Stage 1 sub_department (with the Carlos
                       correction to (VP Sales) since Stage 1's random
                       placement was a known artifact).
  - new_job_title    : derived from at-hire dept + sub_dept + level via
                       the Job Architecture title map.

The lateral transfers tracked in 2c reconcile the Stage 1 -> current
sub-department gap; downstream substages (which produce Promotion and
Lateral Transfer rows) consume those events.

Title map sourced from the Ref - Job Architecture tab of the data
dictionary. Cells the architecture leaves implicit (typically IC1 for
sub-departments where only IC2+ is staffed, or IC5 for sub-departments
without a Section 3 IC5 cell) get extrapolated titles in the same
naming pattern; these only show up in the residual 7-cell delta from
2c.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date
from pathlib import Path
import runpy

import pandas as pd

CURRENT_DATE = date(2025, 3, 31)

# Hardcoded leadership reporting chain per Section 2 of the spec.
# Same as Substage 2b.
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

CARLOS_EMPLOYEE_ID = "EMP-009"
CARLOS_AT_HIRE_SUBDEPT = "(VP Sales)"

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

LEVEL_ORDER = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "M3", "M4", "M5"]
LEVEL_INDEX = {level: i for i, level in enumerate(LEVEL_ORDER)}

# ---------------------------------------------------------------------------
# Title map (Ref - Job Architecture tab)
# ---------------------------------------------------------------------------

TITLE_MAP: dict[tuple[str, str, str], str] = {
    # ENGINEERING (Technical) -- IC1/IC2 share the all-Eng title at those levels
    ("Engineering", "Platform", "IC1"):       "Associate Software Engineer",
    ("Engineering", "Platform", "IC2"):       "Software Engineer",
    ("Engineering", "Platform", "IC3"):       "Senior Software Engineer",
    ("Engineering", "Platform", "IC4"):       "Staff Software Engineer",
    ("Engineering", "Platform", "IC5"):       "Principal Engineer",
    ("Engineering", "AI/ML", "IC1"):          "Associate Software Engineer",
    ("Engineering", "AI/ML", "IC2"):          "Software Engineer",
    ("Engineering", "AI/ML", "IC3"):          "Senior ML Engineer",
    ("Engineering", "AI/ML", "IC4"):          "Staff ML Engineer",
    ("Engineering", "AI/ML", "IC5"):          "Principal ML Engineer",
    ("Engineering", "Data", "IC1"):           "Associate Software Engineer",
    ("Engineering", "Data", "IC2"):           "Software Engineer",
    ("Engineering", "Data", "IC3"):           "Senior Data Engineer",
    ("Engineering", "Data", "IC4"):           "Staff Data Engineer",
    ("Engineering", "Data", "IC5"):           "Principal Data Engineer",
    ("Engineering", "Infrastructure", "IC1"): "Associate Software Engineer",
    ("Engineering", "Infrastructure", "IC2"): "Software Engineer",
    ("Engineering", "Infrastructure", "IC3"): "Senior Software Engineer",
    ("Engineering", "Infrastructure", "IC4"): "Staff Software Engineer",
    ("Engineering", "Infrastructure", "IC5"): "Principal Engineer",
    # PRODUCT & DESIGN (Technical-Adjacent)
    ("Product", "Product Management", "IC1"): "Associate Product Manager",
    ("Product", "Product Management", "IC2"): "Product Manager",
    ("Product", "Product Management", "IC3"): "Senior Product Manager",
    ("Product", "Product Management", "IC4"): "Staff Product Manager",
    ("Product", "Design", "IC1"):             "Associate Product Designer",
    ("Product", "Design", "IC2"):             "Product Designer",
    ("Product", "Design", "IC3"):             "Senior Product Designer",
    ("Product", "Design", "IC4"):             "Staff Product Designer",
    ("Product", "Design", "IC5"):             "Principal Product Designer",
    ("Product", "UX Research", "IC1"):        "Associate UX Researcher",
    ("Product", "UX Research", "IC2"):        "UX Researcher",
    ("Product", "UX Research", "IC3"):        "Senior UX Researcher",
    ("Product", "UX Research", "IC4"):        "Staff UX Researcher",
    # SALES (Commercial)
    ("Sales", "SDR", "IC1"):                  "Sales Development Representative",
    ("Sales", "SDR", "IC2"):                  "Senior SDR",
    ("Sales", "SDR", "IC3"):                  "Staff SDR",
    ("Sales", "SDR", "IC4"):                  "Lead SDR",
    ("Sales", "Account Executive", "IC1"):    "Junior Account Executive",
    ("Sales", "Account Executive", "IC2"):    "Account Executive",
    ("Sales", "Account Executive", "IC3"):    "Senior Account Executive",
    ("Sales", "Account Executive", "IC4"):    "Enterprise Account Executive",
    ("Sales", "Account Management", "IC1"):   "Junior Account Manager",
    ("Sales", "Account Management", "IC2"):   "Account Manager",
    ("Sales", "Account Management", "IC3"):   "Senior Account Manager",
    ("Sales", "Account Management", "IC4"):   "Strategic Account Manager",
    ("Sales", "Sales Engineering", "IC1"):    "Junior Sales Engineer",
    ("Sales", "Sales Engineering", "IC2"):    "Sales Engineer",
    ("Sales", "Sales Engineering", "IC3"):    "Senior Sales Engineer",
    ("Sales", "Sales Engineering", "IC4"):    "Staff Sales Engineer",
    # CUSTOMER SUCCESS (Commercial-Adjacent)
    ("Customer Success", "CSM", "IC1"):       "Junior Customer Success Manager",
    ("Customer Success", "CSM", "IC2"):       "Customer Success Manager",
    ("Customer Success", "CSM", "IC3"):       "Senior CSM",
    ("Customer Success", "CSM", "IC4"):       "Strategic CSM",
    ("Customer Success", "Support", "IC1"):   "Customer Support Specialist",
    ("Customer Success", "Support", "IC2"):   "Senior Support Specialist",
    ("Customer Success", "Support", "IC3"):   "Staff Support Specialist",
    ("Customer Success", "Support", "IC4"):   "Support Lead",
    ("Customer Success", "Implementation", "IC1"): "Junior Implementation Specialist",
    ("Customer Success", "Implementation", "IC2"): "Implementation Specialist",
    ("Customer Success", "Implementation", "IC3"): "Senior Implementation Specialist",
    ("Customer Success", "Implementation", "IC4"): "Implementation Lead",
    ("Customer Success", "Implementation", "IC5"): "Principal Implementation Lead",
    # MARKETING
    ("Marketing", "Growth", "IC1"):           "Marketing Coordinator",
    ("Marketing", "Growth", "IC2"):           "Growth Marketing Manager",
    ("Marketing", "Growth", "IC3"):           "Senior Growth Marketing Manager",
    ("Marketing", "Growth", "IC4"):           "Staff Growth Marketing Manager",
    ("Marketing", "Content", "IC1"):          "Marketing Coordinator",
    ("Marketing", "Content", "IC2"):          "Content Marketing Manager",
    ("Marketing", "Content", "IC3"):          "Senior Content Marketing Manager",
    ("Marketing", "Content", "IC4"):          "Staff Content Strategist",
    ("Marketing", "Product Marketing", "IC1"):"Marketing Coordinator",
    ("Marketing", "Product Marketing", "IC2"):"Product Marketing Manager",
    ("Marketing", "Product Marketing", "IC3"):"Senior Product Marketing Manager",
    ("Marketing", "Product Marketing", "IC4"):"Staff Product Marketing Manager",
    ("Marketing", "Brand", "IC1"):            "Marketing Coordinator",
    ("Marketing", "Brand", "IC2"):            "Brand Manager",
    ("Marketing", "Brand", "IC3"):            "Senior Brand Manager",
    ("Marketing", "Brand", "IC4"):            "Staff Brand Manager",
    # G&A (Operations)
    ("G&A", "Finance", "IC1"):                "Financial Analyst I",
    ("G&A", "Finance", "IC2"):                "Financial Analyst II",
    ("G&A", "Finance", "IC3"):                "Senior Financial Analyst",
    ("G&A", "Finance", "IC4"):                "Staff Financial Analyst",
    ("G&A", "Legal", "IC1"):                  "Legal Coordinator",
    ("G&A", "Legal", "IC2"):                  "Paralegal",
    ("G&A", "Legal", "IC3"):                  "Corporate Counsel",
    ("G&A", "Legal", "IC4"):                  "Senior Corporate Counsel",
    ("G&A", "IT", "IC1"):                     "IT Support Specialist",
    ("G&A", "IT", "IC2"):                     "Systems Administrator",
    ("G&A", "IT", "IC3"):                     "Senior Systems Administrator",
    ("G&A", "IT", "IC4"):                     "IT Manager (IC track)",
    ("G&A", "Facilities", "IC1"):             "Office Coordinator",
    ("G&A", "Facilities", "IC2"):             "Facilities Specialist",
    ("G&A", "Facilities", "IC3"):             "Senior Facilities Specialist",
    ("G&A", "Facilities", "IC4"):             "Staff Facilities Specialist",
    # PEOPLE / HR
    ("People", "Recruiting", "IC1"):          "Recruiting Coordinator",
    ("People", "Recruiting", "IC2"):          "Recruiter",
    ("People", "Recruiting", "IC3"):          "Senior Recruiter",
    ("People", "Recruiting", "IC4"):          "Staff Recruiter",
    ("People", "HRBPs", "IC1"):               "HR Coordinator",
    ("People", "HRBPs", "IC2"):               "HR Business Partner",
    ("People", "HRBPs", "IC3"):               "Senior HRBP",
    ("People", "HRBPs", "IC4"):               "Principal HRBP",
    ("People", "People Ops", "IC1"):          "People Operations Coordinator",
    ("People", "People Ops", "IC2"):          "People Operations Specialist",
    ("People", "People Ops", "IC3"):          "Senior People Operations Specialist",
    ("People", "Total Rewards", "IC1"):       "Total Rewards Coordinator",
    ("People", "Total Rewards", "IC2"):       "Compensation Analyst",
    ("People", "Total Rewards", "IC3"):       "Senior Compensation Analyst",
    ("People", "Total Rewards", "IC4"):       "Compensation Manager (IC track)",
    ("People", "L&D", "IC1"):                 "L&D Coordinator",
    ("People", "L&D", "IC2"):                 "L&D Specialist",
    ("People", "L&D", "IC3"):                 "Senior L&D Specialist",
    ("People", "DEIB", "IC1"):                "DEIB Coordinator",
    ("People", "DEIB", "IC2"):                "DEIB Specialist",
    ("People", "DEIB", "IC3"):                "Senior DEIB Specialist",
}

# Manager-level titles by department + level. Sub-department is
# typically "All <Dept>" in the Job Architecture for these roles.
MANAGER_TITLE_BY_DEPT_LEVEL: dict[tuple[str, str], str] = {
    ("Engineering", "M1"):       "Engineering Manager",
    ("Engineering", "M2"):       "Senior Engineering Manager",
    ("Sales", "M1"):             "Sales Manager",
    ("Sales", "M2"):             "Senior Sales Manager",
    ("Customer Success", "M1"):  "CS Manager",
    ("Customer Success", "M2"):  "Senior CS Manager",
    ("Marketing", "M1"):         "Marketing Manager",
    ("Marketing", "M2"):         "Senior Marketing Manager",
    ("Product", "M1"):           "Product Manager, Lead",
    ("Product", "M2"):           "Senior Manager, Product",
}

# G&A and People have sub-dept-specific manager titles.
SUBDEPT_MANAGER_TITLE: dict[tuple[str, str, str], str] = {
    ("G&A", "Finance", "M1"):           "Finance Manager",
    ("G&A", "Legal", "M1"):             "Legal Manager",
    ("G&A", "IT", "M1"):                "IT Manager",
    ("G&A", "Facilities", "M2"):        "Facilities Manager",
    ("People", "Recruiting", "M1"):     "Recruiting Manager",
    ("People", "HRBPs", "M1"):          "HRBP Manager",
    ("People", "L&D", "M1"):            "L&D Manager",
    ("People", "DEIB", "M2"):           "DEIB Manager",
    ("Sales", "SDR", "M1"):             "SDR Manager",
    ("Sales", "Account Executive", "M1"):"Sales Manager",
    ("Sales", "Account Management", "M1"):"Sales Manager",
    ("Sales", "Sales Engineering", "M1"):"Sales Manager",
}


RAW_EMPLOYEES_COLUMNS = [
    "employee_id",
    "first_name", "last_name", "email",
    "department", "sub_department", "job_title", "job_level",
    "hire_date", "termination_date", "termination_type", "termination_reason",
    "employment_status",
    "manager_id",
    "location_city", "location_state",
    "race_ethnicity", "gender",
    "is_critical_talent",
]

JOB_HISTORY_COLUMNS = [
    "employee_id", "effective_date", "change_type",
    "old_job_level",     "new_job_level",
    "old_department",    "new_department",
    "old_sub_department","new_sub_department",
    "old_job_title",     "new_job_title",
    "old_manager_id",    "new_manager_id",
]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _load_state() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Run all upstream stages; return (profiles, designations, assignments, demographics)."""
    base = Path(__file__).parent
    stage1 = runpy.run_path(str(base / "01_generate_employee_profiles.py"), run_name="stage1")
    stage2a = runpy.run_path(str(base / "02_designate_manager_layer.py"), run_name="stage2a")
    stage2c = runpy.run_path(str(base / "05_align_subdept_level_grid.py"), run_name="stage2c")
    stage2d = runpy.run_path(str(base / "06_build_demographics.py"), run_name="stage2d")
    profiles = stage1["build_employee_profiles"]()
    designations = stage2a["build_level_designations"]()
    assignments_df, _, _ = stage2c["build_aligned_grid"]()
    demographics = stage2d["build_employee_demographics"]()
    return profiles, designations, assignments_df, demographics


# ---------------------------------------------------------------------------
# Title derivation
# ---------------------------------------------------------------------------

def _derive_job_title(
    department: str,
    sub_department: str | None,
    level: str,
    leadership_title: str | None = None,
) -> str:
    """Return job_title for the given (dept, sub_dept, level) combination."""
    if leadership_title:
        return leadership_title

    # Manager titles
    if level in {"M1", "M2"}:
        sub_specific = SUBDEPT_MANAGER_TITLE.get((department, sub_department, level))
        if sub_specific:
            return sub_specific
        dept_title = MANAGER_TITLE_BY_DEPT_LEVEL.get((department, level))
        if dept_title:
            return dept_title

    # IC titles
    title = TITLE_MAP.get((department, sub_department, level))
    if title:
        return title

    # Fallback patterns for cells the architecture leaves implicit.
    if level.startswith("IC"):
        prefix = {"IC1": "Associate", "IC3": "Senior", "IC4": "Staff", "IC5": "Principal"}.get(level)
        suffix = sub_department or department
        return f"{prefix} {suffix}".strip() if prefix else suffix
    return f"{level} {sub_department or department}"


# ---------------------------------------------------------------------------
# Manager_id resolution (current + at-hire)
# ---------------------------------------------------------------------------

def _is_manager_active_at(manager: dict, ref_date: date) -> bool:
    """Was `manager` active on `ref_date`?"""
    if manager["hire_date"] > ref_date:
        return False
    if manager["employment_status"] == "Active":
        return True
    return manager["termination_date"] >= ref_date


def _resolve_manager_id_for(
    reportee: dict,
    state_by_id: dict[str, dict],
    by_dept_sub_level: dict[tuple[str, str, str], list[dict]],
    by_dept_level: dict[tuple[str, str], list[dict]],
    span_count: dict[str, int],
    ref_date: date,
) -> str | None:
    """Find best manager for `reportee` valid as-of `ref_date`."""
    reporter_level = reportee["state_level"]
    target_levels = ACCEPTABLE_MANAGER_LEVELS.get(reporter_level, ())
    if not target_levels:
        return None

    for manager_level in target_levels:
        for tier_candidates in (
            by_dept_sub_level.get(
                (reportee["state_dept"], reportee["state_sub"], manager_level), []
            ),
            by_dept_level.get(
                (reportee["state_dept"], manager_level), []
            ),
        ):
            valid = [
                m for m in tier_candidates
                if m["employee_id"] != reportee["employee_id"]
                and (m["hire_date"] <= ref_date)
                and _is_manager_active_at(m, ref_date)
            ]
            if not valid:
                continue
            valid.sort(key=lambda m: span_count[m["employee_id"]])
            return valid[0]["employee_id"]

    # Cross-dept fallback (rare; only when no in-dept manager exists yet).
    for manager_level in target_levels:
        candidates = [
            m for m in state_by_id.values()
            if m["state_level"] == manager_level
            and m["employee_id"] != reportee["employee_id"]
            and (m["hire_date"] <= ref_date)
            and _is_manager_active_at(m, ref_date)
        ]
        if not candidates:
            continue
        candidates.sort(key=lambda m: span_count[m["employee_id"]])
        return candidates[0]["employee_id"]

    return None


def _resolve_manager_ids(
    state_by_id: dict[str, dict],
    ref_date_fn,
    processing_order: tuple[str, ...] = (
        "M2", "M1", "IC5", "IC4", "IC3", "IC2", "IC1",
    ),
) -> dict[str, str | None]:
    """Resolve manager_id for every profile using the level cascade.

    `state_by_id` carries a `state_dept`, `state_sub`, `state_level`
    triple per profile; the indexes are built only over manager-level
    profiles. `ref_date_fn(reportee_dict) -> date` returns the date at
    which the manager must be active (today for active/current, hire
    date for at-hire).
    """
    by_dept_sub_level: dict[tuple[str, str, str], list[dict]] = defaultdict(list)
    by_dept_level: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for profile in state_by_id.values():
        level = profile["state_level"]
        if not isinstance(level, str) or not level.startswith("M"):
            continue
        by_dept_sub_level[
            (profile["state_dept"], profile["state_sub"], level)
        ].append(profile)
        by_dept_level[(profile["state_dept"], level)].append(profile)

    manager_id_map: dict[str, str | None] = {}
    span_count: dict[str, int] = defaultdict(int)

    # 1. Leadership chain (hardcoded).
    for emp_id, manager_id in LEADERSHIP_MANAGER_IDS.items():
        manager_id_map[emp_id] = manager_id
        if manager_id is not None:
            span_count[manager_id] += 1

    # 2. Process remaining profiles top-down by current level. Within a
    # level, oldest hire_date first stabilizes round-robin.
    for current_level in processing_order:
        cohort = sorted(
            (
                p for p in state_by_id.values()
                if p["state_level"] == current_level
                and p["employee_id"] not in manager_id_map
            ),
            key=lambda p: p["hire_date"],
        )
        for reportee in cohort:
            ref_date = ref_date_fn(reportee)
            manager_id = _resolve_manager_id_for(
                reportee=reportee,
                state_by_id=state_by_id,
                by_dept_sub_level=by_dept_sub_level,
                by_dept_level=by_dept_level,
                span_count=span_count,
                ref_date=ref_date,
            )
            manager_id_map[reportee["employee_id"]] = manager_id
            if manager_id is not None:
                span_count[manager_id] += 1

    return manager_id_map


# ---------------------------------------------------------------------------
# State construction
# ---------------------------------------------------------------------------

def _build_current_state_by_id(
    profiles: pd.DataFrame, assignments_df: pd.DataFrame
) -> dict[str, dict]:
    """Return employee_id -> dict with state_dept/sub/level + hire/term info."""
    assign_lookup = assignments_df.set_index("employee_id").to_dict("index")
    state: dict[str, dict] = {}
    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        assignment = assign_lookup.get(emp_id, {})
        if profile["employment_status"] == "Active" and assignment.get("track") == "active":
            dept = assignment["department"]
            sub = assignment["final_sub_department"]
            level = assignment["final_job_level"]
        else:
            # Terminated / not-in-2c-output: use Stage 1 starting state.
            dept = profile["department"]
            sub = profile["sub_department"]
            level = profile["starting_job_level"]
        state[emp_id] = {
            "employee_id":     emp_id,
            "state_dept":      dept,
            "state_sub":       sub,
            "state_level":     level,
            "hire_date":       profile["hire_date"].date()
                if isinstance(profile["hire_date"], pd.Timestamp) else profile["hire_date"],
            "termination_date": (
                profile["termination_date"].date()
                if isinstance(profile["termination_date"], pd.Timestamp)
                and not pd.isna(profile["termination_date"])
                else None
            ),
            "employment_status": profile["employment_status"],
        }
    return state


def _build_at_hire_state_by_id(
    profiles: pd.DataFrame, designations: pd.DataFrame
) -> dict[str, dict]:
    """Return employee_id -> dict with at-hire state (dept/sub/level)."""
    designation_lookup = designations.set_index("employee_id").to_dict("index")
    state: dict[str, dict] = {}
    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        # At-hire dept (no department transfers in this dataset).
        dept = profile["department"]

        # At-hire sub-department.
        if emp_id == CARLOS_EMPLOYEE_ID:
            sub = CARLOS_AT_HIRE_SUBDEPT
        else:
            sub = profile["sub_department"]

        # At-hire level.
        if emp_id in designation_lookup:
            # 2a designations: the effective_starting_level captures
            # what they actually started at (M1 for external direct,
            # IC3/IC4 for internal promotions, founder IC starting).
            level = designation_lookup[emp_id]["effective_starting_level"]
        elif profile["archetype"] == "Manager step-back":
            # Step-back archetype always started IC3 per Section 5,
            # then was promoted to M1, then stepped back to IC4.
            level = "IC3"
        elif profile["is_leadership"]:
            level = profile["starting_job_level"]
        else:
            level = profile["starting_job_level"]

        state[emp_id] = {
            "employee_id":     emp_id,
            "state_dept":      dept,
            "state_sub":       sub,
            "state_level":     level,
            "hire_date":       profile["hire_date"].date()
                if isinstance(profile["hire_date"], pd.Timestamp) else profile["hire_date"],
            "termination_date": (
                profile["termination_date"].date()
                if isinstance(profile["termination_date"], pd.Timestamp)
                and not pd.isna(profile["termination_date"])
                else None
            ),
            "employment_status": profile["employment_status"],
        }
    return state


# ---------------------------------------------------------------------------
# Top-level builders
# ---------------------------------------------------------------------------

def build_raw_employees() -> pd.DataFrame:
    """Build the 18-column raw_employees DataFrame."""
    profiles, designations, assignments_df, demographics = _load_state()

    current_state = _build_current_state_by_id(profiles, assignments_df)

    def current_ref_date(reportee: dict) -> date:
        if reportee["employment_status"] == "Active":
            return CURRENT_DATE
        return reportee["termination_date"]

    current_manager_ids = _resolve_manager_ids(current_state, current_ref_date)

    leadership_titles = {
        row["employee_id"]: row["starting_job_title"]
        for _, row in profiles.iterrows()
        if row["is_leadership"]
    }
    demo_lookup = demographics.set_index("employee_id").to_dict("index")

    rows: list[dict] = []
    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        state = current_state[emp_id]
        demo = demo_lookup.get(emp_id, {})

        title = _derive_job_title(
            state["state_dept"], state["state_sub"], state["state_level"],
            leadership_title=leadership_titles.get(emp_id),
        )

        rows.append({
            "employee_id":        emp_id,
            "first_name":         demo.get("first_name"),
            "last_name":          demo.get("last_name"),
            "email":              demo.get("email"),
            "department":         state["state_dept"],
            "sub_department":     state["state_sub"],
            "job_title":          title,
            "job_level":          state["state_level"],
            "hire_date":          profile["hire_date"],
            "termination_date":   profile["termination_date"],
            "termination_type":   profile["termination_type"],
            "termination_reason": profile["termination_reason"],
            "employment_status":  profile["employment_status"],
            "manager_id":         current_manager_ids.get(emp_id),
            "location_city":      demo.get("location_city"),
            "location_state":     demo.get("location_state"),
            "race_ethnicity":     demo.get("race_ethnicity"),
            "gender":             demo.get("gender"),
            "is_critical_talent": demo.get("is_critical_talent"),
        })

    return pd.DataFrame(rows, columns=RAW_EMPLOYEES_COLUMNS)


def build_raw_job_history_hire_rows() -> pd.DataFrame:
    """Build the Hire-row subset of raw_job_history (one row per employee)."""
    profiles, designations, _, _ = _load_state()
    at_hire_state = _build_at_hire_state_by_id(profiles, designations)

    def at_hire_ref_date(reportee: dict) -> date:
        return reportee["hire_date"]

    hire_time_manager_ids = _resolve_manager_ids(at_hire_state, at_hire_ref_date)

    leadership_titles = {
        row["employee_id"]: row["starting_job_title"]
        for _, row in profiles.iterrows()
        if row["is_leadership"]
    }

    rows: list[dict] = []
    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        state = at_hire_state[emp_id]

        title = _derive_job_title(
            state["state_dept"], state["state_sub"], state["state_level"],
            leadership_title=leadership_titles.get(emp_id),
        )

        rows.append({
            "employee_id":        emp_id,
            "effective_date":     profile["hire_date"],
            "change_type":        "Hire",
            "old_job_level":      None,
            "new_job_level":      state["state_level"],
            "old_department":     None,
            "new_department":     state["state_dept"],
            "old_sub_department": None,
            "new_sub_department": state["state_sub"],
            "old_job_title":      None,
            "new_job_title":      title,
            "old_manager_id":     None,
            "new_manager_id":     hire_time_manager_ids.get(emp_id),
        })

    return pd.DataFrame(rows, columns=JOB_HISTORY_COLUMNS)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(
    raw_employees: pd.DataFrame, hire_rows: pd.DataFrame
) -> None:
    print(f"\n=== raw_employees ===")
    print(f"  total rows:         {len(raw_employees)}")
    print(f"  columns:            {len(raw_employees.columns)}")
    print(f"  unique employee_id: {raw_employees['employee_id'].nunique()}")
    print(f"  unique email:       {raw_employees['email'].nunique()}")

    null_counts = raw_employees.isna().sum()
    expected_nulls = {
        "termination_date": (raw_employees["employment_status"] == "Active").sum(),
        "termination_type": (raw_employees["employment_status"] == "Active").sum(),
        "termination_reason": (raw_employees["employment_status"] == "Active").sum(),
        "manager_id": 1,  # Maya only
    }
    print("\n  Null counts (and expected):")
    for col, observed in null_counts.items():
        if observed == 0:
            continue
        expected = expected_nulls.get(col, "—")
        marker = "OK" if expected == "—" or observed == expected else f"expected {expected}"
        print(f"    {col:<22} {observed:>4}  {marker}")

    print("\n  Active vs Terminated:")
    print(raw_employees["employment_status"].value_counts().to_string())

    print("\n  Department distribution:")
    print(raw_employees["department"].value_counts().to_string())

    print("\n  Job level distribution:")
    by_level = (
        raw_employees.groupby(["job_level", "employment_status"])
        .size()
        .unstack(fill_value=0)
    )
    print(by_level.to_string())

    print("\n  Top 10 job titles:")
    print(raw_employees["job_title"].value_counts().head(10).to_string())

    print("\n  Manager span of control (current):")
    span = (
        raw_employees[raw_employees["manager_id"].notna()]
        .groupby("manager_id")
        .size()
        .rename("reports")
    )
    span_with_level = span.to_frame().join(
        raw_employees.set_index("employee_id")[["job_level"]],
        how="left",
    )
    print(
        span_with_level
        .groupby("job_level")["reports"]
        .agg(["count", "min", "median", "max", "mean"])
        .round(1)
        .to_string()
    )

    print("\n  Sample raw_employees rows:")
    print(raw_employees.head(3).to_string())

    print(f"\n=== raw_job_history (Hire rows only) ===")
    print(f"  total rows:    {len(hire_rows)}")
    print(f"  change_types:  {hire_rows['change_type'].value_counts().to_dict()}")

    print("\n  Hire rows: counts of new_manager_id null vs not-null:")
    print(f"    null new_manager_id: {hire_rows['new_manager_id'].isna().sum()}  (expected 1: Maya)")
    print(f"    valid new_manager_id: {hire_rows['new_manager_id'].notna().sum()}")

    print("\n  new_job_level distribution at hire (vs current state):")
    crosstab = pd.crosstab(
        hire_rows["new_job_level"],
        raw_employees.set_index("employee_id").loc[hire_rows["employee_id"]]["job_level"].values,
    )
    print(crosstab.to_string())

    print("\n  Sample Hire rows:")
    print(hire_rows.head(3).to_string())


if __name__ == "__main__":
    raw_employees = build_raw_employees()
    hire_rows = build_raw_job_history_hire_rows()
    print_summary(raw_employees, hire_rows)
