"""
Stage 3 deliverable 4: raw_recruiting (Ashby).

Purpose
-------
For every hire that post-dates the company's ATS adoption, generate a
realistic candidate funnel: one Hired row plus rejected / withdrawn /
declined-offer applications across the five funnel stages. Total
volume targets the spec's ~8,000-10,000 rows.

Inputs
------
- Stage 1 profiles (hire_date, archetype, dept)
- Stage 2e raw_employees (recruiter / hiring manager names)
- Stage 8 raw_job_history (Hire row's at-hire dept / sub_dept / level
  / job_title / manager_id)

Outputs
-------
build_raw_recruiting() returns a DataFrame with the 17 columns from
the data dictionary.

ATS adoption cutoff
-------------------
Section 12: "Not every employee needs an Ashby record (founders and
very early hires may predate the ATS)". Q1 2020 hires (Maya, David,
Priya, plus the 9 founders placed Jan 16+) are excluded; the company
adopted Ashby ~Q2 2020 onward in the synthetic narrative. Kevin Zhao
(2020-06-01) and later are included.

Funnel sizing per requisition
-----------------------------
Per hire we generate approximately:
    1     Hired
    ~0.18 Offer Declined          (15% of hires had a separate offer that
                                   was declined first)
    ~2    Rejected at Onsite      (Section 9 funnel)
    ~2.5  Rejected at Technical
    ~5    Rejected at Phone Screen
    ~5    Rejected at Applied stage (capped to keep totals in spec range)

Average ~15.7 rows per hire * 592 ATS-era hires ~= 9,300 rows.
Section 9's stage conversion rates produce a theoretical 47.5
candidates per hire (1 / 0.021); the cap on Applied-stage rejections
trades off realism for the spec's row-count target.

Source distribution
-------------------
Per-department weights from Section 9. Top-two sources per spec are
preserved with the remainder distributed across the smaller channels
to sum to 1.0.

Rejection reasons by stage
--------------------------
From Section 9. "Withdrew" stage-rejection rolls produce
current_stage = "Withdrawn" instead of "Rejected" so the schema
reflects the candidate exit type accurately.
"""

from __future__ import annotations

import random
import runpy
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

RANDOM_SEED = 20260425

ATS_CUTOFF = date(2020, 4, 1)  # Q1 2020 hires predate Ashby

RECRUITING_COLUMNS = [
    "application_id",
    "requisition_id",
    "candidate_name",
    "job_title",
    "department",
    "sub_department",
    "recruiter",
    "hiring_manager",
    "source",
    "current_stage",
    "application_date",
    "phone_screen_date",
    "onsite_date",
    "offer_date",
    "hire_date",
    "offer_accepted",
    "rejection_reason",
]

# Per-department source distribution (sums to 1.0). Top two channels
# from Section 9 are highlighted; remainder distributed across the
# spec's other channels.
DEPT_SOURCE_DIST: dict[str, dict[str, float]] = {
    "Engineering":      {"Referral": 0.35, "LinkedIn": 0.28, "Career Page": 0.13, "Job Board": 0.10, "Agency": 0.07, "Event": 0.04, "Internal": 0.03},
    "Sales":            {"LinkedIn": 0.40, "Job Board": 0.20, "Referral": 0.15, "Agency": 0.10, "Career Page": 0.08, "Event": 0.04, "Internal": 0.03},
    "Customer Success": {"Career Page": 0.25, "LinkedIn": 0.25, "Referral": 0.18, "Job Board": 0.15, "Agency": 0.07, "Event": 0.07, "Internal": 0.03},
    "Marketing":        {"LinkedIn": 0.30, "Referral": 0.25, "Career Page": 0.15, "Job Board": 0.12, "Agency": 0.08, "Event": 0.07, "Internal": 0.03},
    "Product":          {"Referral": 0.30, "LinkedIn": 0.28, "Career Page": 0.15, "Job Board": 0.10, "Agency": 0.08, "Event": 0.06, "Internal": 0.03},
    "G&A":              {"Job Board": 0.28, "Career Page": 0.25, "LinkedIn": 0.22, "Referral": 0.13, "Agency": 0.05, "Event": 0.04, "Internal": 0.03},
    "People":           {"LinkedIn": 0.35, "Referral": 0.25, "Career Page": 0.15, "Job Board": 0.10, "Agency": 0.07, "Event": 0.05, "Internal": 0.03},
}

# Stage rejection reasons from Section 9. Withdrew variants tag the
# candidate as Withdrawn rather than Rejected.
REJECTION_REASONS_BY_STAGE: dict[str, list[tuple[str, float, str]]] = {
    "Phone Screen": [
        ("Not Qualified",         0.40, "Rejected"),
        ("Compensation Mismatch", 0.25, "Rejected"),
        ("Poor Communication",    0.20, "Rejected"),
        ("Withdrew",              0.15, "Withdrawn"),
    ],
    "Technical": [
        ("Failed Technical", 0.55, "Rejected"),
        ("Withdrew",         0.25, "Withdrawn"),
        ("Not Qualified",    0.20, "Rejected"),
    ],
    "Onsite": [
        ("Culture Fit",                 0.30, "Rejected"),
        ("Failed Technical",            0.25, "Rejected"),
        ("Withdrew",                    0.25, "Withdrawn"),
        ("Went with Another Offer",     0.20, "Withdrawn"),
    ],
    "Offer": [
        ("Declined - Compensation",         0.45, "Withdrawn"),
        ("Declined - Accepted Other Offer", 0.35, "Withdrawn"),
        ("Declined - Personal Reasons",     0.20, "Withdrawn"),
    ],
    # Applied-stage exits don't get specific Section 9 reasons; use a
    # plausible mix of recruiter-screen rejections and unresponsive
    # candidates.
    "Applied": [
        ("Not Qualified", 0.50, "Rejected"),
        ("No Response",   0.35, "Withdrawn"),
        ("Withdrew",      0.15, "Withdrawn"),
    ],
}

# Average rejection counts per stage per hire. Tuned to land the total
# row count near the spec's ~8,000-10,000 target while preserving
# Section 9's stage conversion shape.
STAGE_AVG_REJECTIONS: dict[str, float] = {
    "Offer":        0.18,
    "Onsite":       2.0,
    "Technical":    2.5,
    "Phone Screen": 5.0,
    "Applied":      5.0,
}

# Wide candidate-name pools (allow collisions; realistic for a 9k-row
# applicant table).
FIRST_NAMES_POOL = [
    "Aaron", "Abby", "Adam", "Adrian", "Alex", "Alice", "Allison", "Amber", "Amir", "Andrea",
    "Angela", "Anthony", "Arjun", "Ashley", "Ben", "Beth", "Brad", "Brandon", "Brenda", "Brian",
    "Caitlin", "Cameron", "Carlos", "Carmen", "Cassidy", "Chad", "Charles", "Christine", "Chris", "Claire",
    "Clara", "Cody", "Cole", "Connor", "Courtney", "Cynthia", "Daniel", "Dana", "Daria", "David",
    "Diane", "Diego", "Dominic", "Donald", "Doug", "Drew", "Dylan", "Eddie", "Edward", "Elaine",
    "Eli", "Elise", "Elizabeth", "Emma", "Eric", "Erica", "Erin", "Ethan", "Eva", "Fatima",
    "Felix", "Frank", "Gabriel", "Gabriella", "Gail", "Gary", "Gerald", "Grace", "Grant", "Greg",
    "Hailey", "Hannah", "Harold", "Harper", "Hector", "Holly", "Hugo", "Ian", "Ines", "Iris",
    "Isaac", "Isabella", "Jack", "Jacob", "Jada", "Jamal", "Jane", "Jasmine", "Jason", "Javier",
    "Jenna", "Jeremy", "Jessica", "Jian", "Jin", "Joel", "John", "Jorge", "Jordan", "Joseph",
    "Joshua", "Juan", "Julia", "Justin", "Kaitlyn", "Kara", "Karen", "Kareem", "Kate", "Katie",
    "Keith", "Kelly", "Kenji", "Kevin", "Kirsten", "Kristin", "Kyle", "Lakshmi", "Lance", "Laura",
    "Lauren", "Layla", "Leah", "Leo", "Leon", "Liam", "Lily", "Linda", "Lisa", "Logan",
    "Lucas", "Luis", "Madison", "Maggie", "Marcus", "Maria", "Mariana", "Mark", "Martin", "Matthew",
    "Megan", "Mia", "Michael", "Miguel", "Molly", "Monica", "Naomi", "Nate", "Natalie", "Nathan",
    "Neha", "Nicole", "Noah", "Nora", "Olivia", "Omar", "Pablo", "Patricia", "Patrick", "Paul",
    "Peter", "Priya", "Quinn", "Rachel", "Rafael", "Raj", "Rebecca", "Reese", "Riley", "Rita",
    "Robert", "Rohan", "Roman", "Rosa", "Ruby", "Ryan", "Sabrina", "Sage", "Sam", "Samantha",
    "Sara", "Sebastian", "Seth", "Shane", "Shannon", "Sofia", "Sophie", "Spencer", "Stacy", "Stephanie",
    "Steven", "Susan", "Tara", "Taylor", "Teresa", "Thomas", "Tia", "Tina", "Tom", "Tracy",
    "Tyler", "Valerie", "Vanessa", "Vivian", "Wesley", "William", "Wyatt", "Xiao", "Yara", "Yasmin",
    "Yu", "Zach", "Zara", "Zoe",
]
LAST_NAMES_POOL = [
    "Adams", "Allen", "Alvarez", "Anderson", "Bailey", "Baker", "Banerjee", "Bell", "Bennett", "Brooks",
    "Brown", "Bryant", "Cabrera", "Campbell", "Carter", "Castro", "Chan", "Chang", "Chen", "Chowdhury",
    "Clark", "Cohen", "Collins", "Cooper", "Cox", "Cruz", "Davis", "Delgado", "Diaz", "Duarte",
    "Edwards", "Espinoza", "Evans", "Farrell", "Fernandez", "Flores", "Foster", "Garcia", "Gomez", "Gonzalez",
    "Graham", "Gray", "Green", "Griffin", "Hall", "Hamilton", "Hanson", "Harris", "Hayes", "Hernandez",
    "Higgins", "Hill", "Holland", "Hsu", "Huang", "Hughes", "Ibrahim", "Ingram", "Iqbal", "Iyer",
    "Jackson", "Jain", "James", "Jenkins", "Johnson", "Jones", "Kapoor", "Kaur", "Kelly", "Khan",
    "Kim", "King", "Kowalski", "Krishnan", "Kumar", "Lam", "Larsen", "Lee", "Lewis", "Liu",
    "Long", "Lopez", "Marquez", "Martinez", "Mason", "Mehta", "Miller", "Mitchell", "Moore", "Morales",
    "Morgan", "Morris", "Murphy", "Nakamura", "Nakashima", "Nelson", "Nguyen", "Nguyen", "Norris", "Ortiz",
    "Ostrowski", "Owens", "Park", "Patel", "Paterson", "Perez", "Peterson", "Phillips", "Powell", "Price",
    "Quinn", "Ramirez", "Ramos", "Reed", "Reyes", "Rivera", "Roberts", "Rodriguez", "Rogers", "Rosario",
    "Ross", "Russo", "Sanchez", "Santiago", "Sato", "Schmidt", "Sharma", "Shah", "Silva", "Singh",
    "Smith", "Soto", "Stewart", "Sullivan", "Suzuki", "Tanaka", "Tang", "Taylor", "Thomas", "Thompson",
    "Torres", "Tran", "Turner", "Vargas", "Vasquez", "Walker", "Wang", "Watson", "Webb", "Weber",
    "White", "Wilkinson", "Williams", "Wilson", "Wong", "Wright", "Wu", "Yamamoto", "Yang", "Yi",
    "Young", "Zhang", "Zhao", "Zhou",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_date(value) -> date:
    if isinstance(value, pd.Timestamp):
        return value.date()
    return value


def _load_state():
    """Run upstream stages and return profiles, raw_employees, raw_job_history."""
    base = Path(__file__).parent
    stage1 = runpy.run_path(str(base / "01_generate_employee_profiles.py"), run_name="stage1")
    stage2e = runpy.run_path(str(base / "07_materialize_raw_employees.py"), run_name="stage2e")
    stage3a = runpy.run_path(str(base / "08_complete_raw_job_history.py"), run_name="stage3a")
    return (
        stage1["build_employee_profiles"](),
        stage2e["build_raw_employees"](),
        stage3a["build_raw_job_history"](),
    )


def _weighted_choice(rng: random.Random, distribution: dict[str, float]) -> str:
    keys = list(distribution.keys())
    weights = list(distribution.values())
    return rng.choices(keys, weights=weights, k=1)[0]


def _pick_rejection(
    rng: random.Random, stage: str
) -> tuple[str, str]:
    """Return (rejection_reason, current_stage_value)."""
    options = REJECTION_REASONS_BY_STAGE[stage]
    keys = [opt[0] for opt in options]
    weights = [opt[1] for opt in options]
    chosen = rng.choices(range(len(options)), weights=weights, k=1)[0]
    reason, _, exit_state = options[chosen]
    return reason, exit_state


def _poisson_count(rng: random.Random, mean: float) -> int:
    """Approximate Poisson draw (clamped at 0). Uses gauss(mean, sqrt(mean))."""
    if mean <= 0:
        return 0
    sd = max(0.5, mean ** 0.5)
    value = round(rng.gauss(mean, sd))
    return max(0, value)


def _sample_candidate_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES_POOL)} {rng.choice(LAST_NAMES_POOL)}"


def _pick_recruiter(
    rng: random.Random,
    raw_employees: pd.DataFrame,
    requisition_open_date: date,
) -> str:
    """Pick a recruiter active when the requisition was open.

    Recruiters live in People / Recruiting. For requisitions that
    pre-date the first Recruiting hire, fall back to "Talent Acquisition"
    as a placeholder.
    """
    candidates = raw_employees[
        (raw_employees["department"] == "People")
        & (raw_employees["sub_department"] == "Recruiting")
    ].copy()
    candidates["hire_date_d"] = pd.to_datetime(candidates["hire_date"]).dt.date
    candidates["term_date_d"] = pd.to_datetime(candidates["termination_date"]).dt.date
    eligible = candidates[
        (candidates["hire_date_d"] <= requisition_open_date)
        & (
            candidates["term_date_d"].isna()
            | (candidates["term_date_d"] >= requisition_open_date)
        )
    ]
    if len(eligible) == 0:
        return "Talent Acquisition Team"
    pick = eligible.sample(1, random_state=rng.randint(0, 1_000_000))
    row = pick.iloc[0]
    return f"{row['first_name']} {row['last_name']}"


# ---------------------------------------------------------------------------
# Per-requisition application generator
# ---------------------------------------------------------------------------

def _build_hired_application(
    rng: random.Random,
    application_id_seq: list[int],
    requisition_id: str,
    profile: dict,
    hire_row: dict,
    raw_employees: pd.DataFrame,
    employee_name: str,
) -> tuple[dict, date, date, date, date, str]:
    """Build the Hired row plus return the timeline anchors for this requisition."""
    hire_d = _to_date(profile["hire_date"])
    application_d = hire_d - timedelta(days=rng.randint(45, 90))
    phone_d = application_d + timedelta(days=rng.randint(5, 14))
    onsite_d = phone_d + timedelta(days=rng.randint(7, 21))
    offer_d = onsite_d + timedelta(days=rng.randint(3, 10))

    department = hire_row["new_department"]
    sub_department = hire_row["new_sub_department"]
    job_title = hire_row["new_job_title"]
    source = _weighted_choice(rng, DEPT_SOURCE_DIST.get(department, DEPT_SOURCE_DIST["Engineering"]))

    application_id_seq[0] += 1
    application_id = f"APP-{application_id_seq[0]:05d}"

    manager_id = hire_row["new_manager_id"]
    if manager_id is None or pd.isna(manager_id):
        hiring_manager = "Maya Chen"
    else:
        mgr_rows = raw_employees[raw_employees["employee_id"] == manager_id]
        if mgr_rows.empty:
            hiring_manager = "Maya Chen"
        else:
            mgr = mgr_rows.iloc[0]
            hiring_manager = f"{mgr['first_name']} {mgr['last_name']}"

    recruiter = _pick_recruiter(rng, raw_employees, application_d)

    row = {
        "application_id":    application_id,
        "requisition_id":    requisition_id,
        "candidate_name":    employee_name,
        "job_title":         job_title,
        "department":        department,
        "sub_department":    sub_department,
        "recruiter":         recruiter,
        "hiring_manager":    hiring_manager,
        "source":            source,
        "current_stage":     "Hired",
        "application_date":  application_d,
        "phone_screen_date": phone_d,
        "onsite_date":       onsite_d,
        "offer_date":        offer_d,
        "hire_date":         hire_d,
        "offer_accepted":    True,
        "rejection_reason":  None,
    }
    return row, application_d, phone_d, onsite_d, offer_d, recruiter


def _build_rejected_application(
    rng: random.Random,
    application_id_seq: list[int],
    requisition_id: str,
    job_title: str,
    department: str,
    sub_department: str,
    recruiter: str,
    hiring_manager: str,
    requisition_open_date: date,
    hired_offer_date: date,
    stage: str,
) -> dict:
    """Build one rejected/withdrawn application that exited at `stage`."""
    application_id_seq[0] += 1
    application_id = f"APP-{application_id_seq[0]:05d}"

    application_d = requisition_open_date + timedelta(
        days=rng.randint(-30, 30)
    )
    if application_d > hired_offer_date:
        application_d = hired_offer_date - timedelta(days=rng.randint(7, 30))

    phone_d = onsite_d = offer_d = None
    rejection_reason, exit_state = _pick_rejection(rng, stage)

    if stage in ("Phone Screen", "Technical", "Onsite", "Offer"):
        phone_d = application_d + timedelta(days=rng.randint(5, 14))
    if stage in ("Onsite", "Offer"):
        onsite_d = phone_d + timedelta(days=rng.randint(7, 21))
    if stage == "Offer":
        offer_d = onsite_d + timedelta(days=rng.randint(3, 10))

    offer_accepted: bool | None
    if stage == "Offer":
        offer_accepted = False  # offer made but declined
    else:
        offer_accepted = None

    source = _weighted_choice(rng, DEPT_SOURCE_DIST.get(department, DEPT_SOURCE_DIST["Engineering"]))

    return {
        "application_id":    application_id,
        "requisition_id":    requisition_id,
        "candidate_name":    _sample_candidate_name(rng),
        "job_title":         job_title,
        "department":        department,
        "sub_department":    sub_department,
        "recruiter":         recruiter,
        "hiring_manager":    hiring_manager,
        "source":            source,
        "current_stage":     exit_state,
        "application_date":  application_d,
        "phone_screen_date": phone_d,
        "onsite_date":       onsite_d,
        "offer_date":        offer_d,
        "hire_date":         None,
        "offer_accepted":    offer_accepted,
        "rejection_reason":  rejection_reason,
    }


def _generate_for_requisition(
    rng: random.Random,
    application_id_seq: list[int],
    requisition_id: str,
    profile: pd.Series,
    hire_row: dict,
    raw_employees: pd.DataFrame,
) -> list[dict]:
    """Generate the full applicant set for one hire's requisition."""
    employee_name = (
        f"{raw_employees.loc[raw_employees['employee_id'] == profile['employee_id'], 'first_name'].iloc[0]} "
        f"{raw_employees.loc[raw_employees['employee_id'] == profile['employee_id'], 'last_name'].iloc[0]}"
    )

    hired_row, application_d, phone_d, onsite_d, offer_d, recruiter = _build_hired_application(
        rng=rng,
        application_id_seq=application_id_seq,
        requisition_id=requisition_id,
        profile=profile.to_dict(),
        hire_row=hire_row,
        raw_employees=raw_employees,
        employee_name=employee_name,
    )

    rows = [hired_row]
    department = hired_row["department"]
    sub_department = hired_row["sub_department"]
    job_title = hired_row["job_title"]
    hiring_manager = hired_row["hiring_manager"]

    for stage, mean_count in STAGE_AVG_REJECTIONS.items():
        n = _poisson_count(rng, mean_count)
        for _ in range(n):
            rows.append(_build_rejected_application(
                rng=rng,
                application_id_seq=application_id_seq,
                requisition_id=requisition_id,
                job_title=job_title,
                department=department,
                sub_department=sub_department,
                recruiter=recruiter,
                hiring_manager=hiring_manager,
                requisition_open_date=application_d,
                hired_offer_date=offer_d,
                stage=stage,
            ))

    return rows


# ---------------------------------------------------------------------------
# Top-level builder
# ---------------------------------------------------------------------------

def build_raw_recruiting() -> pd.DataFrame:
    profiles, raw_employees, job_history = _load_state()
    rng = random.Random(RANDOM_SEED)

    hire_rows = (
        job_history[job_history["change_type"] == "Hire"]
        .set_index("employee_id")
        .to_dict("index")
    )

    requisition_seq = 0
    application_id_seq = [0]
    all_rows: list[dict] = []

    profiles_sorted = profiles.sort_values("hire_date").reset_index(drop=True)
    for _, profile in profiles_sorted.iterrows():
        emp_id = profile["employee_id"]
        hire_d = _to_date(profile["hire_date"])
        if hire_d < ATS_CUTOFF:
            continue
        hire_row = hire_rows.get(emp_id)
        if hire_row is None:
            continue
        requisition_seq += 1
        requisition_id = f"REQ-{requisition_seq:04d}"
        rows = _generate_for_requisition(
            rng=rng,
            application_id_seq=application_id_seq,
            requisition_id=requisition_id,
            profile=profile,
            hire_row=hire_row,
            raw_employees=raw_employees,
        )
        all_rows.extend(rows)

    df = pd.DataFrame(all_rows, columns=RECRUITING_COLUMNS)
    df["application_date"] = pd.to_datetime(df["application_date"])
    df["phone_screen_date"] = pd.to_datetime(df["phone_screen_date"])
    df["onsite_date"] = pd.to_datetime(df["onsite_date"])
    df["offer_date"] = pd.to_datetime(df["offer_date"])
    df["hire_date"] = pd.to_datetime(df["hire_date"])
    df = df.sort_values(["requisition_id", "application_date"]).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_summary(df: pd.DataFrame) -> None:
    print(f"\n=== raw_recruiting ===")
    print(f"  total rows:           {len(df)}")
    print(f"  unique application_id: {df['application_id'].nunique()}")
    print(f"  unique requisition_id: {df['requisition_id'].nunique()}")

    print("\n  current_stage distribution:")
    print(df["current_stage"].value_counts().to_string())

    print("\n  Funnel (by current_stage end-state):")
    hired_count = (df["current_stage"] == "Hired").sum()
    rejected_count = (df["current_stage"] == "Rejected").sum()
    withdrawn_count = (df["current_stage"] == "Withdrawn").sum()
    total = len(df)
    print(f"    Hired:     {hired_count:>5}  ({hired_count/total*100:.1f}%)")
    print(f"    Rejected:  {rejected_count:>5}  ({rejected_count/total*100:.1f}%)")
    print(f"    Withdrawn: {withdrawn_count:>5}  ({withdrawn_count/total*100:.1f}%)")

    print("\n  Source distribution overall (% of applications):")
    print(df["source"].value_counts(normalize=True).round(3).mul(100).to_string())

    print("\n  Source distribution by department (top 3 per dept):")
    for dept in sorted(df["department"].unique()):
        sub = df[df["department"] == dept]
        top = sub["source"].value_counts(normalize=True).round(3).mul(100).head(3)
        print(f"    {dept}:")
        for source, pct in top.items():
            print(f"      {source}: {pct:.1f}%")

    print("\n  Funnel conversion (computed from data):")
    n_app = len(df)
    n_phone = df["phone_screen_date"].notna().sum()
    n_onsite = df["onsite_date"].notna().sum()
    n_offer = df["offer_date"].notna().sum()
    n_hired = (df["current_stage"] == "Hired").sum()
    print(f"    Applied:           {n_app}")
    print(f"    Phone Screen:      {n_phone}  ({n_phone/n_app*100:.1f}% of Applied)")
    print(f"    Onsite:            {n_onsite}  ({n_onsite/n_phone*100:.1f}% of Phone)")
    print(f"    Offer:             {n_offer}  ({n_offer/n_onsite*100:.1f}% of Onsite)")
    print(f"    Hired:             {n_hired}  ({n_hired/n_offer*100:.1f}% of Offer)")
    print(f"    Overall:           {n_hired/n_app*100:.2f}% Applied -> Hired (target 2.1%)")

    print("\n  Date coherence checks:")
    bad_app = (df["application_date"] >= df["hire_date"]).sum()
    print(f"    application_date >= hire_date: {bad_app}")
    bad_phone = (
        df["phone_screen_date"].notna()
        & (df["phone_screen_date"] < df["application_date"])
    ).sum()
    print(f"    phone_screen_date before application: {bad_phone}")
    bad_offer = (
        df["offer_date"].notna()
        & df["hire_date"].notna()
        & (df["offer_date"] > df["hire_date"])
    ).sum()
    print(f"    offer_date after hire_date: {bad_offer}")

    print("\n  Rejection reasons by stage (top 3):")
    for stage in ["Phone Screen", "Technical", "Onsite", "Offer", "Applied"]:
        sub = df[
            df["current_stage"].isin(["Rejected", "Withdrawn"])
            & df["rejection_reason"].notna()
        ]
        # Rough stage attribution by which dates are filled
        if stage == "Phone Screen":
            sub = sub[sub["phone_screen_date"].notna() & sub["onsite_date"].isna()]
        elif stage == "Technical":
            sub = sub[
                sub["phone_screen_date"].notna() & sub["onsite_date"].isna()
                & (sub["rejection_reason"].isin(["Failed Technical", "Withdrew", "Not Qualified"]))
            ]
        elif stage == "Onsite":
            sub = sub[sub["onsite_date"].notna() & sub["offer_date"].isna()]
        elif stage == "Offer":
            sub = sub[sub["offer_date"].notna() & sub["hire_date"].isna()]
        elif stage == "Applied":
            sub = sub[sub["phone_screen_date"].isna()]
        if len(sub) == 0:
            continue
        print(f"    {stage} ({len(sub)} rows):")
        for reason, count in sub["rejection_reason"].value_counts().head(3).items():
            print(f"      {reason}: {count}")

    print("\n  Sample requisition timeline (first hire):")
    first_req = df["requisition_id"].iloc[0]
    sample = df[df["requisition_id"] == first_req].sort_values("application_date")
    print(
        sample[[
            "application_id", "candidate_name", "current_stage",
            "application_date", "phone_screen_date", "onsite_date",
            "offer_date", "hire_date", "rejection_reason",
        ]].to_string(index=False)
    )


if __name__ == "__main__":
    df = build_raw_recruiting()
    print_summary(df)
