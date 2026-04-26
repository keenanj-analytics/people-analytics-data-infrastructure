"""
Substage 2d: Demographics, locations, names/emails, and is_critical_talent.

Purpose
-------
Populate the remaining raw_employees fields per spec:
    - first_name, last_name, email
    - gender                   (Section 8 distributions per department)
    - race_ethnicity           (Section 8 distributions per department)
    - location_city, location_state  (Section 8 location distribution)
    - is_critical_talent       (Section 5 archetype rules)

Inputs
------
- Stage 1: build_employee_profiles()
- Stage 2a: build_level_designations()  (founder IC critical_talent flag)

Outputs
-------
build_employee_demographics() returns a DataFrame keyed by employee_id
with the columns listed in DEMOGRAPHICS_COLUMNS. The underlying profile
table is intentionally not mutated; 2e joins demographics to profiles
via employee_id for the raw_employees materialization.

Pipeline placement
------------------
Substage 2d. Runs after 2c. 2e consumes assignments + demographics +
promotion/transfer events to materialize raw_employees and the Hire
rows of raw_job_history.

Spec adherence
--------------
Gender and race/ethnicity are sampled per department weights from
Section 8. Names are drawn from broad first / last name pools without
deliberate alignment to race or ethnicity -- forcing names to "match"
demographic categories produces stereotyping risk and is rarely how
real workforce data behaves. The pools are diverse enough that the
joint distribution looks plausible without prescribing it.

Leadership profiles use their Stage 1 first_name / last_name verbatim
and their gender is hard-coded to match (Section 2 names: Maya, Aisha,
Lisa, Nina, Sarah, Michelle, Hannah, Rachel, Amara, Priya are female;
David, Marcus, James, Kevin, Carlos, Raj, Jordan, Derek, Andre are
male). Race/ethnicity for leadership remains sampled from the
department distribution; the org chart names suggest a plausible mix
already.

is_critical_talent rules:
    Defined leadership                -> True for active, False for terminated
    Founder / early employee (active) -> True (Section 5: "All active
                                          founders: is_critical_talent = TRUE")
    Founder IC track (Stage 2a flag)  -> True (carried forward from 2a)
    High-flyer (active)               -> True for exactly
                                          round(0.40 * count_active_high_flyers)
                                          profiles, deterministically
                                          chosen via random sample. Section 5
                                          says "40% chance"; the exact-count
                                          variant is chosen so the share lands
                                          at 40% and not 2-3 sigma off due to
                                          binomial sampling variance.
    All other archetypes              -> False (active or terminated)
    Any terminated profile            -> False (no critical-talent
                                          designation for departed staff)
"""

from __future__ import annotations

import random
import runpy
from pathlib import Path

import pandas as pd

# Bumped only when 2d's selection logic changes.
RANDOM_SEED = 20260425


# ---------------------------------------------------------------------------
# Section 8 distributions
# ---------------------------------------------------------------------------

GENDER_BY_DEPT: dict[str, dict[str, float]] = {
    "Engineering":      {"Male": 0.62, "Female": 0.33, "Non-Binary": 0.05},
    "Sales":            {"Male": 0.52, "Female": 0.45, "Non-Binary": 0.03},
    "Customer Success": {"Male": 0.40, "Female": 0.56, "Non-Binary": 0.04},
    "Marketing":        {"Male": 0.38, "Female": 0.58, "Non-Binary": 0.04},
    "Product":          {"Male": 0.50, "Female": 0.45, "Non-Binary": 0.05},
    "G&A":              {"Male": 0.42, "Female": 0.55, "Non-Binary": 0.03},
    "People":           {"Male": 0.28, "Female": 0.68, "Non-Binary": 0.04},
    # Section 8 has no Executive row; the only Executive employee (Maya)
    # is hard-coded as Female below, so this mapping is unused but kept
    # for safety if a non-leadership Executive ever appears.
    "Executive":        {"Male": 0.40, "Female": 0.55, "Non-Binary": 0.05},
}

RACE_ETHNICITY_BY_DEPT: dict[str, dict[str, float]] = {
    "Engineering":      {"White": 0.35, "Asian": 0.38, "Hispanic/Latino": 0.10, "Black": 0.07, "Two or More": 0.06, "Other/Decline": 0.04},
    "Sales":            {"White": 0.48, "Asian": 0.12, "Hispanic/Latino": 0.18, "Black": 0.12, "Two or More": 0.06, "Other/Decline": 0.04},
    "Customer Success": {"White": 0.42, "Asian": 0.15, "Hispanic/Latino": 0.18, "Black": 0.14, "Two or More": 0.07, "Other/Decline": 0.04},
    "Marketing":        {"White": 0.45, "Asian": 0.18, "Hispanic/Latino": 0.15, "Black": 0.10, "Two or More": 0.08, "Other/Decline": 0.04},
    "Product":          {"White": 0.38, "Asian": 0.32, "Hispanic/Latino": 0.12, "Black": 0.08, "Two or More": 0.06, "Other/Decline": 0.04},
    "G&A":              {"White": 0.45, "Asian": 0.18, "Hispanic/Latino": 0.15, "Black": 0.12, "Two or More": 0.06, "Other/Decline": 0.04},
    "People":           {"White": 0.35, "Asian": 0.15, "Hispanic/Latino": 0.18, "Black": 0.20, "Two or More": 0.08, "Other/Decline": 0.04},
    "Executive":        {"White": 0.40, "Asian": 0.25, "Hispanic/Latino": 0.15, "Black": 0.10, "Two or More": 0.06, "Other/Decline": 0.04},
}

# Section 8 location distribution. Listed states sum to 0.85; the
# remaining 15% ("Other states") is split across nine common tech-
# adjacent states (OR/FL/PA/AZ/VA/MN/MI/OH/TN/UT) with rough
# proportional weights summing to 0.15.
STATE_DISTRIBUTION: dict[str, float] = {
    "CA": 0.30,
    "NY": 0.15,
    "TX": 0.10,
    "WA": 0.08,
    "CO": 0.05,
    "IL": 0.05,
    "MA": 0.05,
    "GA": 0.04,
    "NC": 0.03,
    "OR": 0.02,
    "FL": 0.02,
    "PA": 0.02,
    "AZ": 0.02,
    "VA": 0.02,
    "MN": 0.01,
    "MI": 0.01,
    "OH": 0.01,
    "TN": 0.01,
    "UT": 0.01,
}

CITIES_BY_STATE: dict[str, list[tuple[str, float]]] = {
    "CA": [("San Francisco", 0.50), ("Los Angeles", 0.30), ("San Diego", 0.20)],
    "NY": [("New York City", 0.70), ("Brooklyn", 0.30)],
    "TX": [("Austin", 0.50), ("Dallas", 0.30), ("Houston", 0.20)],
    "WA": [("Seattle", 0.70), ("Bellevue", 0.30)],
    "CO": [("Denver", 0.60), ("Boulder", 0.40)],
    "IL": [("Chicago", 1.00)],
    "MA": [("Boston", 0.60), ("Cambridge", 0.40)],
    "GA": [("Atlanta", 1.00)],
    "NC": [("Raleigh", 0.55), ("Charlotte", 0.45)],
    "OR": [("Portland", 1.00)],
    "FL": [("Miami", 0.50), ("Orlando", 0.50)],
    "PA": [("Philadelphia", 0.70), ("Pittsburgh", 0.30)],
    "AZ": [("Phoenix", 1.00)],
    "VA": [("Arlington", 0.60), ("Richmond", 0.40)],
    "MN": [("Minneapolis", 1.00)],
    "MI": [("Detroit", 1.00)],
    "OH": [("Columbus", 1.00)],
    "TN": [("Nashville", 1.00)],
    "UT": [("Salt Lake City", 1.00)],
}


# ---------------------------------------------------------------------------
# Name pools (broad and diverse; not aligned to demographics)
# ---------------------------------------------------------------------------

FIRST_NAMES_FEMALE = [
    "Alex", "Aisha", "Ana", "Aria", "Ashley", "Ava", "Brianna", "Camila", "Chloe", "Claire",
    "Daniela", "Diana", "Elena", "Emily", "Emma", "Fatima", "Grace", "Hannah", "Imani", "Isabella",
    "Jasmine", "Jessica", "Julia", "Kate", "Kim", "Laura", "Linda", "Maria", "Mia", "Naomi",
    "Nina", "Olivia", "Priya", "Rachel", "Rebecca", "Sara", "Sofia", "Sophia", "Tara", "Vanessa",
    "Yuki", "Zoe",
]

FIRST_NAMES_MALE = [
    "Aaron", "Andre", "Anthony", "Arjun", "Brian", "Carlos", "Chris", "Daniel", "David", "Eric",
    "Felipe", "Gabriel", "Hassan", "Henry", "Ibrahim", "James", "Jason", "John", "Jorge", "Joshua",
    "Kevin", "Liam", "Lucas", "Marcus", "Mark", "Matt", "Michael", "Miguel", "Nathan", "Noah",
    "Omar", "Oscar", "Patrick", "Raj", "Robert", "Ryan", "Samuel", "Steven", "Thomas", "Wei",
]

FIRST_NAMES_NONBINARY = [
    "Avery", "Bay", "Cameron", "Casey", "Drew", "Hayden", "Jamie", "Jordan", "Kai", "Kit",
    "Leslie", "Logan", "Morgan", "Quinn", "Reese", "River", "Riley", "Robin", "Rowan", "Sage",
    "Skyler", "Taylor",
]

LAST_NAMES = [
    "Anderson", "Brown", "Campbell", "Chen", "Clark", "Cohen", "Davis", "Diaz", "Edwards", "Fernandez",
    "Foster", "Garcia", "Gonzalez", "Hall", "Harris", "Hernandez", "Hill", "Ito", "Jackson", "Johnson",
    "Jones", "Khan", "Kim", "Kumar", "Lee", "Lewis", "Liu", "Lopez", "Martinez", "Miller",
    "Mitchell", "Moore", "Morris", "Murphy", "Nguyen", "Okafor", "Park", "Patel", "Perez", "Phillips",
    "Ramirez", "Reed", "Rivera", "Robinson", "Rodriguez", "Roy", "Sanchez", "Sato", "Scott", "Sharma",
    "Shah", "Singh", "Smith", "Sullivan", "Tanaka", "Taylor", "Thomas", "Thompson", "Torres", "Walker",
    "Walsh", "Wang", "Watson", "White", "Williams", "Wilson", "Wong", "Wright", "Yang", "Young",
    "Zhang",
]


# ---------------------------------------------------------------------------
# Leadership overrides
# ---------------------------------------------------------------------------

# Hard-coded gender for the 19 Stage 1 leadership profiles (consistent
# with the Section 2 names). Race/ethnicity for leadership remains
# sampled from the department distribution.
LEADERSHIP_GENDER: dict[str, str] = {
    "EMP-001": "Female",  # Maya Chen
    "EMP-002": "Male",    # David Okafor
    "EMP-003": "Male",    # Marcus Lee
    "EMP-004": "Female",  # Aisha Patel
    "EMP-005": "Male",    # James Wallace
    "EMP-006": "Female",  # Rachel Torres
    "EMP-007": "Male",    # Kevin Zhao
    "EMP-008": "Female",  # Amara Johnson
    "EMP-009": "Male",    # Carlos Mendez
    "EMP-010": "Female",  # Lisa Park
    "EMP-011": "Female",  # Nina Okonkwo
    "EMP-012": "Male",    # Raj Gupta
    "EMP-013": "Female",  # Sarah Kim
    "EMP-014": "Male",    # Jordan Brooks (treating as male; Jordan is gender-flexible)
    "EMP-015": "Female",  # Michelle Torres
    "EMP-016": "Male",    # Derek Washington
    "EMP-017": "Female",  # Hannah Lee
    "EMP-018": "Male",    # Andre Williams
    "EMP-019": "Female",  # Priya Sharma
}

HIGH_FLYER_CRITICAL_TALENT_PROBABILITY = 0.40

DEMOGRAPHICS_COLUMNS = [
    "employee_id",
    "first_name",
    "last_name",
    "email",
    "gender",
    "race_ethnicity",
    "location_city",
    "location_state",
    "is_critical_talent",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_state() -> tuple[pd.DataFrame, pd.DataFrame]:
    """Run Stage 1 + Stage 2a in-process; return profiles + designations."""
    base = Path(__file__).parent
    stage1 = runpy.run_path(
        str(base / "01_generate_employee_profiles.py"), run_name="stage1"
    )
    stage2a = runpy.run_path(
        str(base / "02_designate_manager_layer.py"), run_name="stage2a"
    )
    return stage1["build_employee_profiles"](), stage2a["build_level_designations"]()


def _weighted_choice(rng: random.Random, distribution: dict[str, float]) -> str:
    keys = list(distribution.keys())
    weights = list(distribution.values())
    return rng.choices(keys, weights=weights, k=1)[0]


def _email_handle(first: str, last: str) -> str:
    return (
        f"{first.lower()}.{last.lower()}"
        .replace(" ", "")
        .replace("'", "")
    )


def _allocate_unique_email(
    base_handle: str, used: set[str]
) -> str:
    """Return base_handle@justkaizen.ai, appending a numeric suffix on collision."""
    candidate = f"{base_handle}@justkaizen.ai"
    suffix = 2
    while candidate in used:
        candidate = f"{base_handle}{suffix}@justkaizen.ai"
        suffix += 1
    used.add(candidate)
    return candidate


def _is_critical_talent(
    profile: pd.Series,
    is_founder_ic_designated: bool,
    high_flyer_critical_ids: set[str],
) -> bool:
    """Apply the Section 5 critical-talent rules."""
    if profile["employment_status"] != "Active":
        return False
    if profile["is_leadership"]:
        return True
    if profile["archetype"] == "Founder / early employee":
        return True
    if is_founder_ic_designated:
        return True
    if profile["archetype"] == "High-flyer":
        return profile["employee_id"] in high_flyer_critical_ids
    return False


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build_employee_demographics() -> pd.DataFrame:
    """Generate demographics + names + locations + critical_talent for all 604 profiles."""
    profiles, designations = _load_state()
    rng = random.Random(RANDOM_SEED)

    leadership_names: dict[str, tuple[str, str]] = {
        row["employee_id"]: (row["first_name"], row["last_name"])
        for _, row in profiles.iterrows()
        if row["is_leadership"]
    }
    founder_ic_ids: set[str] = set(
        designations[designations["track"] == "founder_ic"]["employee_id"]
    )

    # Pre-select exactly round(0.40 * N) active high-flyers as critical
    # talent so the share lands at the spec's 40% target rather than
    # drifting from binomial sampling. Sample is deterministic via the
    # module seed.
    active_high_flyers = profiles[
        (profiles["archetype"] == "High-flyer")
        & (profiles["employment_status"] == "Active")
    ]
    high_flyer_critical_count = round(
        len(active_high_flyers) * HIGH_FLYER_CRITICAL_TALENT_PROBABILITY
    )
    high_flyer_critical_ids: set[str] = set(
        rng.sample(
            list(active_high_flyers["employee_id"]),
            high_flyer_critical_count,
        )
    )

    used_emails: set[str] = set()
    rows: list[dict] = []
    for _, profile in profiles.iterrows():
        emp_id = profile["employee_id"]
        dept = profile["department"]

        # Gender: leadership is hard-coded; everyone else samples from
        # the dept's Section 8 distribution.
        if emp_id in LEADERSHIP_GENDER:
            gender = LEADERSHIP_GENDER[emp_id]
        else:
            gender = _weighted_choice(rng, GENDER_BY_DEPT[dept])

        # Race / ethnicity: dept distribution from Section 8.
        race = _weighted_choice(rng, RACE_ETHNICITY_BY_DEPT[dept])

        # Location: state then city.
        state = _weighted_choice(rng, STATE_DISTRIBUTION)
        city_options = {city: weight for city, weight in CITIES_BY_STATE[state]}
        city = _weighted_choice(rng, city_options)

        # Name + email.
        if emp_id in leadership_names:
            first, last = leadership_names[emp_id]
        else:
            if gender == "Female":
                first = rng.choice(FIRST_NAMES_FEMALE)
            elif gender == "Male":
                first = rng.choice(FIRST_NAMES_MALE)
            else:
                first = rng.choice(FIRST_NAMES_NONBINARY)
            last = rng.choice(LAST_NAMES)
        email = _allocate_unique_email(_email_handle(first, last), used_emails)

        # is_critical_talent.
        is_critical = _is_critical_talent(
            profile,
            is_founder_ic_designated=(emp_id in founder_ic_ids),
            high_flyer_critical_ids=high_flyer_critical_ids,
        )

        rows.append({
            "employee_id":        emp_id,
            "first_name":         first,
            "last_name":          last,
            "email":              email,
            "gender":             gender,
            "race_ethnicity":     race,
            "location_city":      city,
            "location_state":     state,
            "is_critical_talent": is_critical,
        })

    return pd.DataFrame(rows, columns=DEMOGRAPHICS_COLUMNS)


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_demographics_summary(demographics: pd.DataFrame) -> None:
    """Print review tables: gender by dept, race by dept, location, critical-talent."""
    profiles, _ = _load_state()
    merged = demographics.merge(
        profiles[["employee_id", "department", "archetype", "employment_status",
                   "is_leadership"]],
        on="employee_id",
    )

    print(f"\nTotal demographic rows: {len(demographics)}")
    print(f"  unique emails:        {demographics['email'].nunique()}")
    print(
        f"  email uniqueness:     "
        f"{'OK' if demographics['email'].nunique() == len(demographics) else 'FAIL'}"
    )

    print("\n=== Gender distribution by department (% of dept) ===")
    gender_by_dept = pd.crosstab(
        merged["department"], merged["gender"], normalize="index"
    ).round(3) * 100
    for col in ("Male", "Female", "Non-Binary"):
        if col not in gender_by_dept.columns:
            gender_by_dept[col] = 0.0
    print(gender_by_dept[["Male", "Female", "Non-Binary"]].to_string())

    print("\n   Section 8 targets (Male / Female / Non-Binary):")
    for dept in ["Engineering", "Sales", "Customer Success", "Marketing",
                 "Product", "G&A", "People"]:
        weights = GENDER_BY_DEPT[dept]
        print(
            f"   {dept:<18} "
            f"{int(weights['Male']*100):>5} {int(weights['Female']*100):>5} "
            f"{int(weights['Non-Binary']*100):>5}"
        )

    print("\n=== Race / ethnicity distribution by department (% of dept) ===")
    race_categories = ["White", "Asian", "Hispanic/Latino", "Black",
                       "Two or More", "Other/Decline"]
    race_by_dept = pd.crosstab(
        merged["department"], merged["race_ethnicity"], normalize="index"
    ).round(3) * 100
    for col in race_categories:
        if col not in race_by_dept.columns:
            race_by_dept[col] = 0.0
    print(race_by_dept[race_categories].to_string())

    print("\n=== Location state distribution ===")
    state_counts = (
        demographics["location_state"].value_counts(normalize=True)
        .round(3) * 100
    )
    print(state_counts.to_string())
    print("\n   Section 8 spec headlines:  CA 30, NY 15, TX 10, WA 8, "
          "CO/IL/MA 5 each, GA 4, NC 3, Other 15")

    print("\n=== Top cities ===")
    print(demographics["location_city"].value_counts().head(15).to_string())

    print("\n=== is_critical_talent ===")
    crit = merged["is_critical_talent"]
    print(f"  total True:  {int(crit.sum())}")
    print(f"  total False: {int((~crit).sum())}")
    print()
    print("  by archetype (active only):")
    active = merged[merged["employment_status"] == "Active"]
    by_arch = (
        active.groupby("archetype")["is_critical_talent"]
        .agg(["count", "sum", "mean"])
        .rename(columns={"sum": "true_count", "mean": "true_share"})
    )
    by_arch["true_share"] = (by_arch["true_share"] * 100).round(1)
    print(by_arch.to_string())

    print("\n  by department (active only):")
    print(
        active.groupby("department")["is_critical_talent"]
        .agg(["count", "sum"])
        .rename(columns={"sum": "true_count"})
        .to_string()
    )


if __name__ == "__main__":
    demographics = build_employee_demographics()
    print_demographics_summary(demographics)
