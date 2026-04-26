"""
Substage 2a: M1-M2 manager layer + Founder IC4-IC5 track designation.

Purpose
-------
Produce two related sets of level designations for active non-leadership
profiles, both keyed by employee_id:

    1. Manager layer (49 rows): 38 M1 + 11 M2 sized to the Section 2
       department distribution. Each row records pathway
       (external_direct_m1 ~30% vs internal_promotion ~70%) and the
       effective starting level (M1 for external direct hires; IC3 or
       IC4 for internal promotions).

    2. Founder IC track (10 rows): 5 IC4 + 5 IC5 carved out of the 27
       active Founder / early-employee profiles. These represent the
       "Now ... IC4-IC5" subset of Section 5's founder current-state
       description ("Now M2+ or IC4-IC5"). All 10 are marked
       is_critical_talent = True.

Inputs
------
    Output of `build_employee_profiles()` from
    `01_generate_employee_profiles.py`. Loaded in-process via runpy so
    the dataframe is reproducible from Stage 1's RANDOM_SEED.

Outputs
-------
    `build_level_designations()` returns a single pandas DataFrame
    containing both manager and founder-IC rows, with the columns
    listed in `DESIGNATION_COLUMNS`. Use the `track` column to
    partition. The underlying employee_profiles DataFrame is
    intentionally not mutated; downstream stages join via employee_id.

Pipeline placement
------------------
    Substage 2a. Runs after Stage 1. Outputs feed into 2b (manager_id
    hierarchy resolution), 2c (current job_level alignment to the
    Section 3 sub-dept × level grid), and 2e (raw_employees,
    raw_job_history Hire rows).

Design notes
------------
    Founder IC track (10 carve-outs)
        Selection prefers Stage-1 non-M1-starting founders so that the
        17 manager-track founders absorb most of the M1-starters and
        Stage-1 starting-level overrides stay minimal. Within the 10
        selected, the top 5 by tenure become IC5 (longer service =
        more promotions); the bottom 5 become IC4.

    Manager layer (49 designations)
        Per-dept candidate pool: top (M1+M2) profiles after sorting on
        archetype priority (Founder > High-flyer > Internal mover >
        Steady contributor) then earliest hire_date.

        Pathway: external direct-M1 hires are allocated globally to
        round(49 * 0.30) = 15 and distributed across departments by
        largest-remainder. Within each department, external slots
        prefer Stage-1 M1-starting candidates (to minimize override),
        then random fill from IC starters.

        M2 designation: 11 M2 split as 6 external + 5 internal so the
        M2 pathway mix lands close to 50/50 (vs an all-tenure rule
        which would otherwise concentrate ~9 of 11 M2 in the external
        pathway because long-tenured founders dominate the external
        pool). Per-dept external M2 counts are derived by
        largest-remainder targeting the global 6/5 split, capped by
        each department's external and M2 totals. Within each dept's
        external and internal sub-pools, the longest-tenured profiles
        become M2.
"""

from __future__ import annotations

import random
import runpy
from datetime import date
from pathlib import Path

import pandas as pd

# Bumped only when the designation logic itself changes; Stage 1's seed
# governs the upstream profile distribution.
RANDOM_SEED = 20260425

# Section 2 department × manager-level distribution. (M1 count, M2 count).
SECTION_2_MANAGER_TARGETS: dict[str, tuple[int, int]] = {
    "Engineering":      (14, 4),
    "Sales":             (8, 2),
    "Customer Success":  (4, 1),
    "Marketing":         (3, 1),
    "Product":           (3, 1),
    "G&A":               (3, 1),
    "People":            (3, 1),
}

# Spec: ~30% of M1-M2 managers were external hires directly into M1.
EXTERNAL_DIRECT_M1_SHARE = 0.30

# Of 11 M2 designations, target 6 external + 5 internal (~55/45 split).
# This keeps the M2 pathway mix close to 50/50 while preserving Section
# 2 per-department M2 counts and the ~30% global external pathway.
EXTERNAL_M2_TARGET_TOTAL = 6

# Founder carve-out: 10 active founders → IC track (5 IC4 + 5 IC5).
# Remaining 17 active founders enter the manager pool.
FOUNDER_IC_TRACK_TOTAL = 10
FOUNDER_IC4_COUNT = 5
FOUNDER_IC5_COUNT = 5

# Per-department selection priority for the manager pool. Lower = picked
# earlier when filling manager slots. See module docstring.
ARCHETYPE_PRIORITY: dict[str, int] = {
    "Founder / early employee": 1,
    "High-flyer":               2,
    "Internal mover":           3,
    "Steady contributor":       4,
}

# Profiles that cannot become M1-M2 managers regardless of dept demand.
INELIGIBLE_ARCHETYPES: frozenset[str] = frozenset({
    "Defined leadership",   # already M3 or higher
    "Manager step-back",    # by definition stepped back to IC4
})

CURRENT_DATE = date(2025, 3, 31)

DESIGNATION_COLUMNS = [
    "employee_id",
    "department",
    "sub_department",
    "archetype",
    "track",
    "current_job_level",
    "pathway",
    "effective_starting_level",
    "stage1_starting_level",
    "is_critical_talent",
    "hire_date",
    "tenure_days_at_2025_03_31",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_employee_profiles() -> pd.DataFrame:
    """Run Stage 1 in-process and return its profile DataFrame."""
    stage1_path = Path(__file__).parent / "01_generate_employee_profiles.py"
    namespace = runpy.run_path(str(stage1_path), run_name="stage1")
    return namespace["build_employee_profiles"]()


def _largest_remainder_allocation(
    fractional_shares: dict[str, float], target_total: int
) -> dict[str, int]:
    """Allocate `target_total` integer units across keys using largest-remainder.

    Each key gets floor(share); the residual is distributed by remainder
    descending. Used for both the global external pathway (15 slots) and
    the global external M2 sub-allocation (6 slots).
    """
    floors = {k: int(v) for k, v in fractional_shares.items()}
    remainders = {k: v - int(v) for k, v in fractional_shares.items()}
    bump = target_total - sum(floors.values())
    sorted_remainders = sorted(
        remainders.items(), key=lambda kv: kv[1], reverse=True
    )
    result = dict(floors)
    for key, _ in sorted_remainders[:bump]:
        result[key] += 1
    return result


def _allocate_external_count_per_dept() -> dict[str, int]:
    """Distribute 15 external direct-M1 hires across departments."""
    total_managers = sum(m1 + m2 for m1, m2 in SECTION_2_MANAGER_TARGETS.values())
    target = round(total_managers * EXTERNAL_DIRECT_M1_SHARE)
    fractional = {
        dept: (m1 + m2) * EXTERNAL_DIRECT_M1_SHARE
        for dept, (m1, m2) in SECTION_2_MANAGER_TARGETS.items()
    }
    return _largest_remainder_allocation(fractional, target)


def _allocate_external_m2_per_dept(
    external_per_dept: dict[str, int],
) -> dict[str, int]:
    """Distribute 6 external M2 designations across departments.

    Per-dept allocation uses largest-remainder on M2 share, then is
    clipped so it never exceeds the department's total external count
    (you cannot have more external M2 than external managers in a
    department) or its total M2 count.
    """
    total_m2 = sum(m2 for _, m2 in SECTION_2_MANAGER_TARGETS.values())
    target = EXTERNAL_M2_TARGET_TOTAL
    fractional = {
        dept: m2 * (target / total_m2)
        for dept, (_, m2) in SECTION_2_MANAGER_TARGETS.items()
    }
    proposed = _largest_remainder_allocation(fractional, target)

    capped = {}
    for dept, (_, m2) in SECTION_2_MANAGER_TARGETS.items():
        capped[dept] = min(proposed[dept], external_per_dept[dept], m2)
    if sum(capped.values()) != target:
        # If the clip creates a shortfall (rare given current Section 2
        # numbers but defensive), redistribute among uncapped depts.
        deficit = target - sum(capped.values())
        for dept in capped:
            if deficit == 0:
                break
            _, m2 = SECTION_2_MANAGER_TARGETS[dept]
            headroom = min(external_per_dept[dept], m2) - capped[dept]
            if headroom > 0:
                bump = min(headroom, deficit)
                capped[dept] += bump
                deficit -= bump
    return capped


def _eligible_manager_candidates(
    profiles: pd.DataFrame, excluded_indices: set[int]
) -> pd.DataFrame:
    """Filter to active non-leadership non-step-back, minus excluded indices."""
    eligible = profiles[
        (profiles["employment_status"] == "Active")
        & (~profiles["is_leadership"])
        & (~profiles["archetype"].isin(INELIGIBLE_ARCHETYPES))
        & (~profiles.index.isin(excluded_indices))
    ].copy()
    eligible["priority"] = (
        eligible["archetype"].map(ARCHETYPE_PRIORITY).fillna(99).astype(int)
    )
    eligible["tenure_days"] = (
        pd.Timestamp(CURRENT_DATE) - eligible["hire_date"]
    ).dt.days
    return eligible


# ---------------------------------------------------------------------------
# Founder IC track selection
# ---------------------------------------------------------------------------

def _select_founder_ic_track(
    rng: random.Random, profiles: pd.DataFrame
) -> tuple[list[dict], set[int]]:
    """Pick 10 active founders for the IC track and return (rows, indices).

    Preference: Stage-1 non-M1-starting founders so M1-starters remain
    available for the manager pool's external-direct-M1 pathway. Within
    the selected 10, the longest-tenured 5 become IC5 and the remaining
    5 become IC4.
    """
    founders = profiles[
        (profiles["archetype"] == "Founder / early employee")
        & (profiles["employment_status"] == "Active")
    ].copy()
    if len(founders) < FOUNDER_IC_TRACK_TOTAL:
        raise ValueError(
            f"Need {FOUNDER_IC_TRACK_TOTAL} active founders for the IC track, "
            f"only {len(founders)} available"
        )

    non_m1 = founders[founders["starting_job_level"] != "M1"]
    if len(non_m1) >= FOUNDER_IC_TRACK_TOTAL:
        selected_indices = rng.sample(list(non_m1.index), FOUNDER_IC_TRACK_TOTAL)
    else:
        m1_only = founders[founders["starting_job_level"] == "M1"]
        additional = FOUNDER_IC_TRACK_TOTAL - len(non_m1)
        selected_indices = list(non_m1.index) + rng.sample(
            list(m1_only.index), additional
        )

    selected = founders.loc[selected_indices].sort_values("hire_date")
    ic5_indices = set(selected.head(FOUNDER_IC5_COUNT).index)

    rows: list[dict] = []
    for idx, row in selected.iterrows():
        current_level = "IC5" if idx in ic5_indices else "IC4"
        rows.append({
            "employee_id":               row["employee_id"],
            "department":                row["department"],
            "sub_department":            row["sub_department"],
            "archetype":                 row["archetype"],
            "track":                     "founder_ic",
            "current_job_level":         current_level,
            "pathway":                   None,
            "effective_starting_level":  row["starting_job_level"],
            "stage1_starting_level":     row["starting_job_level"],
            "is_critical_talent":        True,
            "hire_date":                 row["hire_date"],
            "tenure_days_at_2025_03_31": int(
                (pd.Timestamp(CURRENT_DATE) - row["hire_date"]).days
            ),
        })
    return rows, set(selected_indices)


# ---------------------------------------------------------------------------
# Manager layer designation
# ---------------------------------------------------------------------------

def _designate_managers_for_dept(
    rng: random.Random,
    dept_pool: pd.DataFrame,
    external_count: int,
    external_m2_count: int,
    m2_count: int,
) -> list[dict]:
    """Designate one department's manager set with pathway + current_level.

    `dept_pool` must already be the (m1+m2) candidates pre-sorted by
    archetype priority and hire_date. External designations prefer
    Stage-1 M1-starting candidates; remaining external slots are filled
    randomly from IC-starting candidates. Within each pathway pool, the
    longest-tenured profiles become M2 up to the dept's allotted M2
    counts.
    """
    stage1_m1_pool = [
        idx for idx, row in dept_pool.iterrows()
        if row["starting_job_level"] == "M1"
    ]
    if len(stage1_m1_pool) >= external_count:
        external_indices = set(rng.sample(stage1_m1_pool, external_count))
    else:
        ic_pool = [idx for idx in dept_pool.index if idx not in stage1_m1_pool]
        external_indices = set(stage1_m1_pool) | set(
            rng.sample(ic_pool, external_count - len(stage1_m1_pool))
        )
    internal_indices = set(dept_pool.index) - external_indices

    internal_m2_count = m2_count - external_m2_count

    def by_tenure_oldest_first(indices: set[int]) -> list[int]:
        return sorted(indices, key=lambda i: dept_pool.loc[i, "hire_date"])

    external_m2_indices = set(
        by_tenure_oldest_first(external_indices)[:external_m2_count]
    )
    internal_m2_indices = set(
        by_tenure_oldest_first(internal_indices)[:internal_m2_count]
    )
    m2_indices = external_m2_indices | internal_m2_indices

    rows: list[dict] = []
    for idx, row in dept_pool.iterrows():
        is_m2 = idx in m2_indices
        is_external = idx in external_indices

        current_level = "M2" if is_m2 else "M1"
        pathway = "external_direct_m1" if is_external else "internal_promotion"
        if pathway == "external_direct_m1":
            effective_starting = "M1"
        else:
            effective_starting = (
                row["starting_job_level"]
                if row["starting_job_level"] in {"IC3", "IC4"}
                else "IC3"
            )

        rows.append({
            "employee_id":               row["employee_id"],
            "department":                row["department"],
            "sub_department":            row["sub_department"],
            "archetype":                 row["archetype"],
            "track":                     "manager",
            "current_job_level":         current_level,
            "pathway":                   pathway,
            "effective_starting_level":  effective_starting,
            "stage1_starting_level":     row["starting_job_level"],
            # All active founders are critical talent per spec; high-
            # flyer critical-talent flag (40% chance) is set in 2d.
            "is_critical_talent": (
                row["archetype"] == "Founder / early employee"
            ),
            "hire_date":                 row["hire_date"],
            "tenure_days_at_2025_03_31": int(row["tenure_days"]),
        })
    return rows


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build_level_designations() -> pd.DataFrame:
    """Return manager + founder-IC level designations as one DataFrame.

    Pure function: deterministic given RANDOM_SEED and Stage 1 output.
    """
    rng = random.Random(RANDOM_SEED)
    profiles = _load_employee_profiles()

    founder_ic_rows, founder_ic_indices = _select_founder_ic_track(rng, profiles)
    eligible = _eligible_manager_candidates(profiles, founder_ic_indices)

    external_per_dept = _allocate_external_count_per_dept()
    external_m2_per_dept = _allocate_external_m2_per_dept(external_per_dept)

    manager_rows: list[dict] = []
    for dept, (m1_count, m2_count) in SECTION_2_MANAGER_TARGETS.items():
        total = m1_count + m2_count
        dept_pool = (
            eligible[eligible["department"] == dept]
            .sort_values(["priority", "hire_date"])
            .head(total)
            .copy()
        )
        if len(dept_pool) < total:
            raise ValueError(
                f"{dept}: need {total} managers but only {len(dept_pool)} "
                f"eligible candidates available"
            )
        manager_rows.extend(
            _designate_managers_for_dept(
                rng=rng,
                dept_pool=dept_pool,
                external_count=external_per_dept[dept],
                external_m2_count=external_m2_per_dept[dept],
                m2_count=m2_count,
            )
        )

    return pd.DataFrame(
        founder_ic_rows + manager_rows, columns=DESIGNATION_COLUMNS
    )


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_designation_summary(designations: pd.DataFrame) -> None:
    """Print review tables for both tracks."""
    total = len(designations)
    by_track = designations["track"].value_counts()
    print(f"\nTotal level designations: {total}")
    for track, count in by_track.items():
        print(f"  {track}: {count}")

    managers = designations[designations["track"] == "manager"]
    founders = designations[designations["track"] == "founder_ic"]

    # ------- Manager layer -------
    print("\n=== Manager layer ===")
    print(
        f"M1: {(managers['current_job_level'] == 'M1').sum()}   "
        f"M2: {(managers['current_job_level'] == 'M2').sum()}"
    )

    print("\n--- Department × Current Level (actual) ---")
    actual = (
        managers
        .groupby(["department", "current_job_level"])
        .size()
        .unstack(fill_value=0)
        .reindex(SECTION_2_MANAGER_TARGETS.keys())
    )
    for level in ("M1", "M2"):
        if level not in actual.columns:
            actual[level] = 0
    actual = actual[["M1", "M2"]]
    actual["total"] = actual["M1"] + actual["M2"]
    print(actual.to_string())

    print("\n--- Department × Current Level (Section 2 target) ---")
    targets = pd.DataFrame.from_dict(
        SECTION_2_MANAGER_TARGETS, orient="index", columns=["M1", "M2"]
    )
    targets["total"] = targets["M1"] + targets["M2"]
    print(targets.to_string())

    print("\n--- Pathway split ---")
    print(managers["pathway"].value_counts().to_string())
    external_pct = (
        (managers["pathway"] == "external_direct_m1").mean() * 100
    )
    print(f"  external share: {external_pct:.1f}% (target 30.0%)")

    print("\n--- Pathway × Current Level cross-tab ---")
    print(
        pd.crosstab(managers["pathway"], managers["current_job_level"])
        .to_string()
    )

    print("\n--- Source archetype mix (manager pool) ---")
    print(managers["archetype"].value_counts().to_string())

    print("\n--- Tenure (days as of 2025-03-31) ---")
    print(
        managers
        .groupby("current_job_level")["tenure_days_at_2025_03_31"]
        .agg(["min", "median", "max", "count"])
        .to_string()
    )
    print()
    print(
        managers
        .groupby("pathway")["tenure_days_at_2025_03_31"]
        .agg(["min", "median", "max", "count"])
        .to_string()
    )

    print("\n--- starting_level overrides on internal promotions ---")
    internal = managers[managers["pathway"] == "internal_promotion"]
    overrides = internal[
        internal["stage1_starting_level"] != internal["effective_starting_level"]
    ]
    print(f"  total internal promotions: {len(internal)}")
    print(f"  overridden:                {len(overrides)}")
    if len(overrides) > 0:
        print(
            overrides[["stage1_starting_level", "effective_starting_level"]]
            .value_counts()
            .to_string()
        )

    # ------- Founder IC track -------
    print("\n=== Founder IC track (carved out of 27 active founders) ===")
    print(f"Total: {len(founders)} (5 IC4 + 5 IC5 target)")
    print()
    print("--- Current level distribution ---")
    print(founders["current_job_level"].value_counts().to_string())
    print()
    print("--- Department mix ---")
    print(founders["department"].value_counts().to_string())
    print()
    print("--- Tenure (days as of 2025-03-31) ---")
    print(
        founders
        .groupby("current_job_level")["tenure_days_at_2025_03_31"]
        .agg(["min", "median", "max", "count"])
        .to_string()
    )
    print()
    print(
        f"is_critical_talent = True for all {len(founders)} founder-IC profiles: "
        f"{founders['is_critical_talent'].all()}"
    )
    print()
    print("--- Stage-1 starting level distribution ---")
    print(founders["stage1_starting_level"].value_counts().to_string())


if __name__ == "__main__":
    designations = build_level_designations()
    print_designation_summary(designations)
