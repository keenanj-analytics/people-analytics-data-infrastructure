"""
Substage 2c (step 1): Active sub-department x level grid audit.

Purpose
-------
Read-only audit. Compute the current active-employee distribution across
the (department, sub_department, current_job_level) cells and compare to
the Section 3 target grid. Print the gaps so 2c step 2 (reassignment)
can target only the cells that need it.

Inputs
------
- Stage 1: `build_employee_profiles()`
- Stage 2a: `build_level_designations()`

Outputs
-------
Stdout only. No mutations to upstream stages, no CSV writes.

Current-level resolution
------------------------
For active profiles (only those are audited; Section 3 describes
current state):
  * Leadership: starting_job_level (= current for the M3-M5 leaders).
  * 2a designations: current_job_level (M1, M2, IC4, IC5).
  * Manager Step-Back archetype: IC4 override per Section 5.
  * Everyone else: Stage 1 starting_job_level (placeholder; the very
    cells whose deltas we are about to enumerate).

The Executive department is reported separately because Section 3 has
no Executive row -- Maya Chen is the only Executive employee.
"""

from __future__ import annotations

import runpy
from pathlib import Path

import pandas as pd

# Section 3 of the spec: active headcount per (department, sub_department, level).
# Transcribed from the table; includes the leadership-only sub-departments
# such as "(CTO)" and "(VP CS)" so those cells map cleanly to the
# corresponding Stage 1 leadership profiles.
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

LEVELS = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "M3", "M4", "M5"]
IN_SCOPE_DEPARTMENTS = [
    "Engineering", "Sales", "Customer Success", "Marketing",
    "Product", "G&A", "People",
]


def _load_profiles_with_current_level() -> pd.DataFrame:
    """Run Stage 1 + 2a in-process, return profiles with current_job_level resolved."""
    base = Path(__file__).parent
    stage1 = runpy.run_path(
        str(base / "01_generate_employee_profiles.py"), run_name="stage1"
    )
    stage2a = runpy.run_path(
        str(base / "02_designate_manager_layer.py"), run_name="stage2a"
    )
    profiles = stage1["build_employee_profiles"]()
    designations = stage2a["build_level_designations"]()

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

    profiles = profiles.copy()
    profiles["current_job_level"] = profiles["employee_id"].map(level_lookup)
    return profiles


def _build_target_grid() -> pd.DataFrame:
    rows = []
    for (dept, sub_dept), levels in SECTION_3_GRID.items():
        rows.append({"department": dept, "sub_department": sub_dept, **levels})
    return (
        pd.DataFrame(rows)
        .set_index(["department", "sub_department"])[LEVELS]
        .astype(int)
    )


def _build_current_grid(profiles: pd.DataFrame) -> pd.DataFrame:
    active = profiles[
        (profiles["employment_status"] == "Active")
        & (profiles["department"].isin(IN_SCOPE_DEPARTMENTS))
    ]
    grid = (
        active
        .groupby(["department", "sub_department", "current_job_level"])
        .size()
        .unstack(fill_value=0)
    )
    for level in LEVELS:
        if level not in grid.columns:
            grid[level] = 0
    return grid[LEVELS].astype(int)


def audit() -> None:
    profiles = _load_profiles_with_current_level()
    target_grid = _build_target_grid()
    current_grid = _build_current_grid(profiles)

    all_keys = sorted(set(current_grid.index) | set(target_grid.index))
    current_grid = current_grid.reindex(all_keys, fill_value=0)
    target_grid = target_grid.reindex(all_keys, fill_value=0)
    delta_grid = current_grid - target_grid

    executive_active = profiles[
        (profiles["employment_status"] == "Active")
        & (profiles["department"] == "Executive")
    ]
    print(f"\nExecutive department (out of audit scope, tracked separately):")
    print(f"  active count: {len(executive_active)} (Maya Chen, M5 CEO)")

    print("\n=== Per-department totals ===")
    summary = pd.DataFrame({
        "current": current_grid.sum(axis=1).groupby("department").sum(),
        "target":  target_grid.sum(axis=1).groupby("department").sum(),
    })
    summary["delta"] = summary["current"] - summary["target"]
    summary.loc["TOTAL"] = summary.sum(numeric_only=True)
    print(summary.to_string())

    print("\n=== Delta cells (current - target) where delta != 0 ===")
    # Name the column axis so stack().reset_index() produces a stable
    # "level" column regardless of pandas version.
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
    print(nonzero.to_string(index=False))

    surplus = int(nonzero[nonzero["delta"] > 0]["delta"].sum())
    shortage = int(-nonzero[nonzero["delta"] < 0]["delta"].sum())
    print(
        f"\n  cells with non-zero delta: {len(nonzero)} of {len(delta_long)}"
    )
    print(f"  total surplus  (current > target): {surplus}")
    print(f"  total shortage (current < target): {shortage}")
    print(
        f"  net surplus:  {surplus - shortage}  "
        f"(= active in audit {int(current_grid.values.sum())} "
        f"- target {int(target_grid.values.sum())})"
    )

    print("\n=== Delta by department x level (collapsed across sub-departments) ===")
    dept_level = (
        delta_grid.groupby("department").sum().astype(int)
    )
    print(dept_level.to_string())

    print("\n=== Delta by level (collapsed across departments) ===")
    by_level = delta_grid.sum().astype(int).rename("delta_total")
    print(by_level.to_string())


if __name__ == "__main__":
    audit()
