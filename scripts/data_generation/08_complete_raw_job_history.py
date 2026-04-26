"""
Stage 3 deliverable 1: complete raw_job_history.

Purpose
-------
Combine the Hire rows from 2e with every other career-event row that
the data dictionary requires:

    Hire              -- 604 (one per profile, from 2e)
    Promotion         -- IC -> IC and IC -> M and M -> M+1 events
    Lateral Transfer  -- sub-department moves (from 2c rebalance + flex)
    Title Change      -- encodes the Manager Step-Back's M1 -> IC4
                          step-back; no native "Step-back" change_type
                          exists in the data dictionary
    Manager Change    -- Manager Change Casualty (rigid 3-9 months
                          before termination per Section 5) + organic
                          (when the at-hire manager differs from the
                          current/last manager)

The result is one DataFrame ordered by (employee_id, effective_date).

Inputs
------
- Stage 1: build_employee_profiles()
- Stage 2a: build_level_designations()
- Stage 2c: build_aligned_grid() returns (assignments, promotion_events, subdept_events)
- Stage 2e: build_raw_employees() and build_raw_job_history_hire_rows()
            (used to resolve at-hire and current manager_id without
             re-running the resolution logic here)

Outputs
-------
build_raw_job_history() returns a DataFrame with the 13 columns from
the data dictionary plus all six change_types represented.

Spec adherence
--------------
- Sequential level progression: every Promotion event moves exactly
  one level (the IC -> M boundary jumps are treated as one promotion
  per spec narrative).
- No events after termination_date: every effective_date is bounded
  by min(CURRENT_DATE, profile.termination_date).
- Hire row's old_* fields are null. Subsequent rows carry the
  pre-event snapshot in old_* and post-event snapshot in new_*; for
  fields the event does not change, old_* == new_*.
- Lateral Transfers do not implicitly change manager_id in this
  output; if the rebalance also changed manager (e.g. Eng Platform
  -> Eng Data has different M1), the Manager Change event for that
  reportee is generated separately when at-hire and current manager
  differ.

Manager Change timing
---------------------
- Manager Change Casualty: random uniform 90-270 days before
  termination_date (Section 5: "experienced a manager change 3-9
  months before departure").
- Organic changes: midpoint of the tenure (hire_date to
  current_date for active, hire_date to termination_date for
  terminated). This is approximate; a more realistic model would
  anchor on the new manager's hire_date when the new manager is a
  later-arriving leader (e.g. Sales pre-Marcus to Sales post-Marcus
  for early Sales hires). That refinement is deferred.
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

LEVEL_ORDER = ["IC1", "IC2", "IC3", "IC4", "IC5", "M1", "M2", "M3", "M4", "M5"]
LEVEL_INDEX = {level: i for i, level in enumerate(LEVEL_ORDER)}

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

def _load_state():
    """Run upstream stages and return all dataframes + helpers."""
    base = Path(__file__).parent
    stage1 = runpy.run_path(str(base / "01_generate_employee_profiles.py"), run_name="stage1")
    stage2a = runpy.run_path(str(base / "02_designate_manager_layer.py"), run_name="stage2a")
    stage2c = runpy.run_path(str(base / "05_align_subdept_level_grid.py"), run_name="stage2c")
    stage2e = runpy.run_path(str(base / "07_materialize_raw_employees.py"), run_name="stage2e")

    profiles = stage1["build_employee_profiles"]()
    designations = stage2a["build_level_designations"]()
    assignments_df, promotion_events_2c, subdept_change_events_2c = (
        stage2c["build_aligned_grid"]()
    )
    raw_employees = stage2e["build_raw_employees"]()
    hire_rows_df = stage2e["build_raw_job_history_hire_rows"]()
    derive_job_title = stage2e["_derive_job_title"]

    return (
        profiles, designations, assignments_df,
        promotion_events_2c, subdept_change_events_2c,
        raw_employees, hire_rows_df, derive_job_title,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_date(value) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _is_nat_or_none(value) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and pd.isna(value):
        return True
    return isinstance(value, pd.Timestamp) and pd.isna(value)


def _profile_end_date(profile: pd.Series) -> date:
    """Cutoff date for events: today (active) or termination_date (terminated)."""
    if profile["employment_status"] == "Active":
        return CURRENT_DATE
    return _to_date(profile["termination_date"])


def _synthesize_at_hire_manager(
    emp_id: str,
    profile: dict,
    current_mgr_id: str | None,
    profile_by_id: dict[str, dict],
    current_row_lookup: dict[str, dict],
) -> str | None:
    """Find a same-dept M1+ manager (different from current) active at hire.

    Used for Manager Change Casualty profiles whose original at-hire
    manager from 07 happens to match their current manager. Falls back
    to the CEO (cross-department) when no in-dept alternative exists --
    that case is rare but can occur for very early Sales / Marketing /
    People hires before any in-dept manager was hired.
    """
    profile_hire_d = _to_date(profile["hire_date"])
    profile_dept = profile["department"]

    for candidate_id, candidate_profile in profile_by_id.items():
        if candidate_id == current_mgr_id or candidate_id == emp_id:
            continue
        cand_re = current_row_lookup.get(candidate_id)
        if cand_re is None:
            continue
        if cand_re["department"] != profile_dept:
            continue
        cand_level = cand_re["job_level"]
        if not isinstance(cand_level, str) or not cand_level.startswith("M"):
            continue
        cand_hire_d = _to_date(candidate_profile["hire_date"])
        if cand_hire_d > profile_hire_d:
            continue
        if candidate_profile["employment_status"] == "Terminated":
            cand_term = _to_date(candidate_profile["termination_date"])
            if cand_term < profile_hire_d:
                continue
        return candidate_id

    # No same-dept manager available at hire -- fall back to CEO.
    return "EMP-001" if "EMP-001" != current_mgr_id else None


def _spread_dates(start: date, end: date, count: int) -> list[date]:
    """Spread `count` evenly-spaced dates strictly between start and end."""
    if count <= 0:
        return []
    span = max(1, (end - start).days)
    return [
        start + timedelta(days=int(span * (i + 1) / (count + 1)))
        for i in range(count)
    ]


def _level_path(starting_level: str, ending_level: str) -> list[tuple[str, str]]:
    """Return a list of (old, new) one-step level transitions from start to end.

    For IC->IC moves: walks one level at a time (IC2 -> IC3 -> IC4).
    For IC->M moves: collapsed to a single jump (IC3 -> M1, IC4 -> M1)
    per spec narrative; does not walk through intermediate IC levels.
    For M->M moves: single steps (M1 -> M2).
    """
    if starting_level == ending_level:
        return []

    start_idx = LEVEL_INDEX[starting_level]
    end_idx = LEVEL_INDEX[ending_level]
    if end_idx <= start_idx:
        return []

    transitions: list[tuple[str, str]] = []
    current = starting_level

    # IC -> M jump (single-event collapse).
    if start_idx <= LEVEL_INDEX["IC5"] and end_idx >= LEVEL_INDEX["M1"]:
        transitions.append((current, "M1"))
        current = "M1"
    else:
        # IC -> IC walk, one level per event.
        while LEVEL_INDEX[current] < end_idx and current != ending_level:
            next_level = LEVEL_ORDER[LEVEL_INDEX[current] + 1]
            transitions.append((current, next_level))
            current = next_level

    # Manager-track tail: M1 -> M2, M2 -> M3, ...
    while LEVEL_INDEX[current] < end_idx:
        next_level = LEVEL_ORDER[LEVEL_INDEX[current] + 1]
        transitions.append((current, next_level))
        current = next_level

    return transitions


# ---------------------------------------------------------------------------
# Raw event collection
# ---------------------------------------------------------------------------

def _collect_raw_events(
    profiles: pd.DataFrame,
    designations: pd.DataFrame,
    promotion_events_2c: pd.DataFrame,
    subdept_change_events_2c: pd.DataFrame,
    hire_rows_df: pd.DataFrame,
    raw_employees: pd.DataFrame,
    rng: random.Random,
) -> dict[str, list[dict]]:
    """Return employee_id -> list of raw event dicts (unsorted, no state).

    Each event dict has keys: type, effective_date, plus whichever of
    `level`, `sub_dept`, `manager_id` the event changes. The state is
    applied later when rows are emitted.
    """
    events: dict[str, list[dict]] = defaultdict(list)

    profile_by_id = profiles.set_index("employee_id").to_dict("index")
    designation_lookup = designations.set_index("employee_id").to_dict("index")
    hire_row_lookup = hire_rows_df.set_index("employee_id").to_dict("index")
    current_row_lookup = raw_employees.set_index("employee_id").to_dict("index")

    # Adjusted at-hire manager: prefer the current manager when they
    # were already hired by the reportee's hire_date. This eliminates
    # span-balancing churn (where 07 picked manager A at hire-time and
    # manager B at current-time even though both were equally valid),
    # so phantom Manager Change events do not get generated. Manager
    # Change Casualty profiles keep the original at-hire manager so the
    # archetype-driven Manager Change event still has a real diff to
    # encode.
    adjusted_at_hire_manager: dict[str, str | None] = {}
    for emp_id, hire_row in hire_row_lookup.items():
        profile = profile_by_id[emp_id]
        original_hire_mgr = hire_row["new_manager_id"]
        if pd.isna(original_hire_mgr):
            original_hire_mgr = None
        current_mgr = current_row_lookup[emp_id]["manager_id"]
        if pd.isna(current_mgr):
            current_mgr = None

        if profile["archetype"] == "Manager change casualty":
            # MCC must show a manager change in job_history (Section 5
            # archetype rule). When the at-hire manager from 07 happens
            # to equal the current manager, synthesize a different
            # at-hire manager so the Manager Change event has a real
            # diff to encode.
            if original_hire_mgr is not None and original_hire_mgr != current_mgr:
                adjusted_at_hire_manager[emp_id] = original_hire_mgr
            else:
                adjusted_at_hire_manager[emp_id] = _synthesize_at_hire_manager(
                    emp_id, profile, current_mgr, profile_by_id, current_row_lookup
                )
            continue

        if current_mgr is None or current_mgr == original_hire_mgr:
            adjusted_at_hire_manager[emp_id] = original_hire_mgr
            continue

        current_mgr_profile = profile_by_id.get(current_mgr)
        profile_hire_d = _to_date(profile["hire_date"])
        if (
            current_mgr_profile is not None
            and _to_date(current_mgr_profile["hire_date"]) <= profile_hire_d
            # Manager must also have been active at reportee's hire.
            and (
                current_mgr_profile["employment_status"] == "Active"
                or _to_date(current_mgr_profile["termination_date"]) >= profile_hire_d
            )
        ):
            adjusted_at_hire_manager[emp_id] = current_mgr
        else:
            adjusted_at_hire_manager[emp_id] = original_hire_mgr

    # 1. Hire events (one per profile).
    for emp_id, hire_row in hire_row_lookup.items():
        events[emp_id].append({
            "type":           "Hire",
            "effective_date": _to_date(hire_row["effective_date"]),
            "level":          hire_row["new_job_level"],
            "department":     hire_row["new_department"],
            "sub_dept":       hire_row["new_sub_department"],
            "manager_id":     adjusted_at_hire_manager[emp_id],
        })

    # 2. Promotion events from 2c flex (Steady, High-flyer, Internal Mover).
    for _, event in promotion_events_2c.iterrows():
        events[event["employee_id"]].append({
            "type":           "Promotion",
            "effective_date": _to_date(event["effective_date"]),
            "level":          event["new_value"],
        })

    # 3. Promotion events derived from 2a manager-track designations.
    #    Internal promotions: starting IC3/IC4 -> M1 (1 promotion)
    #    or -> M1 -> M2 (2 promotions). External direct M1 hires that
    #    are now M2: starting M1 -> M2 (1 promotion). External direct
    #    M1 hires still M1: 0 promotions.
    for _, designation in designations.iterrows():
        if designation["track"] != "manager":
            continue
        emp_id = designation["employee_id"]
        starting = designation["effective_starting_level"]
        ending = designation["current_job_level"]
        transitions = _level_path(starting, ending)
        if not transitions:
            continue
        profile = profile_by_id[emp_id]
        hire_d = _to_date(profile["hire_date"])
        end_d = _profile_end_date(profile)
        promo_dates = _spread_dates(hire_d, end_d, len(transitions))
        for promo_date, (_old, new) in zip(promo_dates, transitions):
            events[emp_id].append({
                "type":           "Promotion",
                "effective_date": promo_date,
                "level":          new,
            })

    # 4. Promotion events from founder IC track designations.
    for _, designation in designations.iterrows():
        if designation["track"] != "founder_ic":
            continue
        emp_id = designation["employee_id"]
        starting = designation["effective_starting_level"]
        ending = designation["current_job_level"]
        transitions = _level_path(starting, ending)
        if not transitions:
            continue
        profile = profile_by_id[emp_id]
        hire_d = _to_date(profile["hire_date"])
        end_d = _profile_end_date(profile)
        promo_dates = _spread_dates(hire_d, end_d, len(transitions))
        for promo_date, (_old, new) in zip(promo_dates, transitions):
            events[emp_id].append({
                "type":           "Promotion",
                "effective_date": promo_date,
                "level":          new,
            })

    # 5. Manager Step-Back: IC3 -> M1 promotion + M1 -> IC4 step-back.
    #    Spec: "Promoted to M1 after 12-18 months. Stepped back to IC4
    #    within 12 months of M1 promotion." Place promotion at ~30%
    #    tenure, step-back at ~55% tenure (within 12 months of M1).
    for _, profile in profiles.iterrows():
        if profile["archetype"] != "Manager step-back":
            continue
        if profile["employment_status"] != "Active":
            continue
        emp_id = profile["employee_id"]
        hire_d = _to_date(profile["hire_date"])
        end_d = _profile_end_date(profile)
        span = (end_d - hire_d).days
        promo_date = hire_d + timedelta(days=int(span * 0.30))
        step_back_date = hire_d + timedelta(days=int(span * 0.55))
        events[emp_id].append({
            "type":           "Promotion",
            "effective_date": promo_date,
            "level":          "M1",
        })
        events[emp_id].append({
            "type":           "Title Change",
            "effective_date": step_back_date,
            "level":          "IC4",
        })

    # 6. Lateral Transfer events from 2c sub-dept changes.
    for _, event in subdept_change_events_2c.iterrows():
        events[event["employee_id"]].append({
            "type":           "Lateral Transfer",
            "effective_date": _to_date(event["effective_date"]),
            "sub_dept":       event["new_value"],
        })

    # 7. Manager Change events:
    #    7a. Manager Change Casualty (terminated, archetype-driven):
    #        rigid 3-9 months before termination_date.
    #    7b. Organic: at-hire manager differs from current/last manager
    #        for a structural reason (current manager wasn't hired yet
    #        at reportee's hire, or at-hire manager terminated before
    #        reportee's current/end date). Span-balancing churn is
    #        already absorbed by the adjusted at-hire manager above so
    #        all surviving differences here are genuine.
    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        hire_mgr = adjusted_at_hire_manager[emp_id]
        current_mgr = current_row_lookup[emp_id]["manager_id"]
        if pd.isna(current_mgr):
            current_mgr = None
        if hire_mgr == current_mgr:
            continue

        if profile["archetype"] == "Manager change casualty":
            term_d = _to_date(profile["termination_date"])
            change_date = term_d - timedelta(days=rng.randint(90, 270))
        else:
            hire_d = _to_date(profile["hire_date"])
            end_d = _profile_end_date(profile)
            span = max(1, (end_d - hire_d).days)
            change_date = hire_d + timedelta(days=span // 2)
        # Guard the date inside the profile's tenure.
        hire_d = _to_date(profile["hire_date"])
        if change_date <= hire_d:
            change_date = hire_d + timedelta(days=1)
        end_d = _profile_end_date(profile)
        if change_date >= end_d:
            change_date = end_d - timedelta(days=1)

        events[emp_id].append({
            "type":           "Manager Change",
            "effective_date": change_date,
            "manager_id":     current_mgr,
        })

    return events


# ---------------------------------------------------------------------------
# State-tracked row emission
# ---------------------------------------------------------------------------

def _emit_rows_for_profile(
    emp_id: str,
    raw_events: list[dict],
    profile: dict,
    leadership_title: str | None,
    derive_job_title,
) -> list[dict]:
    """Apply events in chronological order; emit one row per event."""
    raw_events.sort(key=lambda e: (e["effective_date"], 0 if e["type"] == "Hire" else 1))

    state = {
        "level":      None,
        "department": None,
        "sub_dept":   None,
        "title":      None,
        "manager_id": None,
    }
    rows: list[dict] = []

    for event in raw_events:
        old_state = dict(state)
        change_type = event["type"]

        if change_type == "Hire":
            state["level"] = event["level"]
            state["department"] = event["department"]
            state["sub_dept"] = event["sub_dept"]
            state["title"] = derive_job_title(
                state["department"], state["sub_dept"], state["level"],
                leadership_title=leadership_title,
            )
            state["manager_id"] = event["manager_id"]
            old_state = {key: None for key in state}

        elif change_type == "Promotion":
            state["level"] = event["level"]
            state["title"] = derive_job_title(
                state["department"], state["sub_dept"], state["level"],
                leadership_title=leadership_title,
            )

        elif change_type == "Lateral Transfer":
            state["sub_dept"] = event["sub_dept"]
            state["title"] = derive_job_title(
                state["department"], state["sub_dept"], state["level"],
                leadership_title=leadership_title,
            )

        elif change_type == "Title Change":
            # Used for Manager Step-Back's M1 -> IC4 demotion.
            state["level"] = event["level"]
            state["title"] = derive_job_title(
                state["department"], state["sub_dept"], state["level"],
                leadership_title=leadership_title,
            )

        elif change_type == "Manager Change":
            state["manager_id"] = event["manager_id"]

        else:
            raise ValueError(f"Unknown change_type {change_type} for {emp_id}")

        rows.append({
            "employee_id":         emp_id,
            "effective_date":      event["effective_date"],
            "change_type":         change_type,
            "old_job_level":       old_state["level"],
            "new_job_level":       state["level"],
            "old_department":      old_state["department"],
            "new_department":      state["department"],
            "old_sub_department":  old_state["sub_dept"],
            "new_sub_department":  state["sub_dept"],
            "old_job_title":       old_state["title"],
            "new_job_title":       state["title"],
            "old_manager_id":      old_state["manager_id"],
            "new_manager_id":      state["manager_id"],
        })
    return rows


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build_raw_job_history() -> pd.DataFrame:
    """Build the complete raw_job_history DataFrame."""
    (
        profiles, designations, assignments_df,
        promotion_events_2c, subdept_change_events_2c,
        raw_employees, hire_rows_df, derive_job_title,
    ) = _load_state()
    rng = random.Random(RANDOM_SEED)

    raw_events_per_profile = _collect_raw_events(
        profiles=profiles,
        designations=designations,
        promotion_events_2c=promotion_events_2c,
        subdept_change_events_2c=subdept_change_events_2c,
        hire_rows_df=hire_rows_df,
        raw_employees=raw_employees,
        rng=rng,
    )

    leadership_titles = {
        row["employee_id"]: row["starting_job_title"]
        for _, row in profiles.iterrows()
        if row["is_leadership"]
    }
    profile_by_id = profiles.set_index("employee_id").to_dict("index")

    all_rows: list[dict] = []
    for emp_id, events in raw_events_per_profile.items():
        rows = _emit_rows_for_profile(
            emp_id=emp_id,
            raw_events=events,
            profile=profile_by_id[emp_id],
            leadership_title=leadership_titles.get(emp_id),
            derive_job_title=derive_job_title,
        )
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows, columns=JOB_HISTORY_COLUMNS)
    df = df.sort_values(["employee_id", "effective_date"]).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame) -> None:
    print(f"\n=== raw_job_history ===")
    print(f"  total rows:       {len(df)}")
    print(f"  unique employees: {df['employee_id'].nunique()}")
    print()
    print("Rows by change_type:")
    print(df["change_type"].value_counts().to_string())

    print("\nRows per employee summary:")
    rows_per = df.groupby("employee_id").size()
    print(
        rows_per.describe().round(1).to_string()
    )

    print("\nEmployees with > 5 events (top 10):")
    top_events = rows_per.sort_values(ascending=False).head(10)
    print(top_events.to_string())

    print("\nHire-row coverage check:")
    hire_count = (df["change_type"] == "Hire").sum()
    print(f"  Hire rows:                 {hire_count}")
    print(f"  Expected (one per profile): {df['employee_id'].nunique()}")
    print(f"  Match: {'OK' if hire_count == df['employee_id'].nunique() else 'FAIL'}")

    print("\nPromotion level transitions:")
    promo = df[df["change_type"] == "Promotion"]
    print(
        promo.groupby(["old_job_level", "new_job_level"]).size().to_string()
    )

    print("\nLateral Transfer count:", (df["change_type"] == "Lateral Transfer").sum())
    print("Manager Change count:  ", (df["change_type"] == "Manager Change").sum())
    print("Title Change count:    ", (df["change_type"] == "Title Change").sum())

    print("\nSequential level progression check (Promotion rows only):")
    bad = []
    for _, row in promo.iterrows():
        old_idx = LEVEL_INDEX.get(row["old_job_level"])
        new_idx = LEVEL_INDEX.get(row["new_job_level"])
        if old_idx is None or new_idx is None:
            continue
        if new_idx <= old_idx:
            bad.append((row["employee_id"], row["old_job_level"], row["new_job_level"]))
        # IC -> IC must be one step. IC -> M is allowed any-IC -> M1.
        if new_idx <= LEVEL_INDEX["IC5"] and new_idx - old_idx > 1:
            bad.append((row["employee_id"], row["old_job_level"], row["new_job_level"]))
    print(f"  out-of-order or skip-level Promotions: {len(bad)}")

    print("\nNo-events-after-termination_date check:")
    profiles_check = (
        df.merge(
            df[df["change_type"] == "Hire"][["employee_id", "effective_date"]]
            .rename(columns={"effective_date": "hire_date"}),
            on="employee_id",
        )
    )
    print(f"  rows checked: {len(profiles_check)}")

    print("\nSample (3 employees with 4+ events):")
    multi_event_ids = rows_per[rows_per >= 4].index[:3]
    sample = df[df["employee_id"].isin(multi_event_ids)].sort_values(
        ["employee_id", "effective_date"]
    )
    print(
        sample[["employee_id", "effective_date", "change_type",
                "old_job_level", "new_job_level",
                "old_sub_department", "new_sub_department",
                "old_manager_id", "new_manager_id"]]
        .to_string(index=False)
    )


if __name__ == "__main__":
    df = build_raw_job_history()
    print_summary(df)
