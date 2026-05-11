"""
generate_all_seeds.py

Synthetic data generator for JustKaizen AI's People Analytics warehouse.

Produces 6 CSV seed files in seeds/ that satisfy the cross-table invariants
required by the dbt models:

    raw_comp_bands      ~200 rows, one per unique job title
    raw_employees       ~1,900 rows (1,200 active + ~700 terminated)
    raw_job_history     ~3-4k rows, one event per (employee, change)
    raw_performance     ~10-15k rows, manager Performance Category only
    raw_offers_hires    ~5-6k rows, one per candidate
    raw_ees_responses   ~150-200k rows, anonymized

Determinism:
    Seeded RNG (random, numpy, faker). Re-runs are byte-identical.

Run:
    python scripts/data_generation/generate_all_seeds.py
"""

from __future__ import annotations

import csv
import hashlib
import random
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

import numpy as np
from faker import Faker

# ---------------------------------------------------------------------------
# Setup & constants
# ---------------------------------------------------------------------------

RNG_SEED = 42
random.seed(RNG_SEED)
np.random.seed(RNG_SEED)
fake = Faker("en_US")
Faker.seed(RNG_SEED)

ROOT = Path(__file__).resolve().parents[2]
SEEDS_DIR = ROOT / "seeds"
SEEDS_DIR.mkdir(exist_ok=True)

DATA_START = date(2018, 1, 1)
DATA_END = date(2025, 3, 1)
REPORT_DATE = date(2025, 3, 1)

ACTIVE_TARGET = 1200
TERM_TARGET = 700
RIF_TARGET = 150  # Q1 2023 RIF, exactly

# ---------------------------------------------------------------------------
# Departments & sub-departments (active counts from company profile §6)
# ---------------------------------------------------------------------------

DEPARTMENTS = {
    "Engineering": {
        "code": "ENG",
        "active": 410,
        "terminated_voluntary": 165,
        "rif": 40,
        "sub_depts": {
            "Platform": 85, "AI/ML": 80, "Infrastructure": 65,
            "Data Engineering": 55, "Security": 35, "QA": 35,
            "Developer Experience": 25, "DevOps & SRE": 30,
        },
        "level_dist": {"P1-P3": 0.42, "P4-P6": 0.28, "M1-M2": 0.20, "M3-M4": 0.08, "E1-E6": 0.02},
        "gender": {"Men": 0.62, "Women": 0.33, "Non-Binary": 0.05},
        "race": {"White": 0.35, "Asian": 0.38, "Hispanic or Latino": 0.10,
                 "Black or African American": 0.07, "Two or More Races": 0.06,
                 "Not Specified": 0.04},
        "multiplier": 1.22,
        "ttf_range": (50, 65),
    },
    "Sales": {
        "code": "SAL", "active": 200, "terminated_voluntary": 130, "rif": 35,
        "sub_depts": {
            "SDR": 55, "Account Executive": 50, "Account Management": 30,
            "Sales Engineering": 25, "Enterprise Sales": 25, "Revenue Operations": 15,
        },
        "level_dist": {"P1-P3": 0.55, "P4-P6": 0.15, "M1-M2": 0.22, "M3-M4": 0.06, "E1-E6": 0.02},
        "gender": {"Men": 0.52, "Women": 0.45, "Non-Binary": 0.03},
        "race": {"White": 0.48, "Asian": 0.12, "Hispanic or Latino": 0.18,
                 "Black or African American": 0.12, "Two or More Races": 0.06,
                 "Not Specified": 0.04},
        "multiplier": 1.00, "ttf_range": (30, 40),
    },
    "Customer Success": {
        "code": "CS", "active": 130, "terminated_voluntary": 70, "rif": 15,
        "sub_depts": {
            "CSM": 45, "Support": 35, "Implementation": 25, "Customer Education": 25,
        },
        "level_dist": {"P1-P3": 0.50, "P4-P6": 0.18, "M1-M2": 0.22, "M3-M4": 0.08, "E1-E6": 0.02},
        "gender": {"Men": 0.40, "Women": 0.56, "Non-Binary": 0.04},
        "race": {"White": 0.42, "Asian": 0.15, "Hispanic or Latino": 0.18,
                 "Black or African American": 0.14, "Two or More Races": 0.07,
                 "Not Specified": 0.04},
        "multiplier": 0.92, "ttf_range": (40, 50),
    },
    "G&A": {
        "code": "GA", "active": 110, "terminated_voluntary": 50, "rif": 18,
        "sub_depts": {
            "Finance": 30, "IT": 25, "Legal": 15, "Business Operations": 20,
            "Facilities": 10, "Accounting": 10,
        },
        "level_dist": {"P1-P3": 0.45, "P4-P6": 0.22, "M1-M2": 0.20, "M3-M4": 0.10, "E1-E6": 0.03},
        "gender": {"Men": 0.42, "Women": 0.55, "Non-Binary": 0.03},
        "race": {"White": 0.45, "Asian": 0.18, "Hispanic or Latino": 0.15,
                 "Black or African American": 0.12, "Two or More Races": 0.06,
                 "Not Specified": 0.04},
        "multiplier": 0.88, "ttf_range": (40, 50),
    },
    "Marketing": {
        "code": "MKT", "active": 100, "terminated_voluntary": 50, "rif": 20,
        "sub_depts": {
            "Growth / Demand Gen": 25, "Content": 20, "Product Marketing": 20,
            "Brand & Creative": 15, "Events & Comms": 10, "Growth Analytics": 10,
        },
        "level_dist": {"P1-P3": 0.45, "P4-P6": 0.20, "M1-M2": 0.22, "M3-M4": 0.10, "E1-E6": 0.03},
        "gender": {"Men": 0.38, "Women": 0.58, "Non-Binary": 0.04},
        "race": {"White": 0.45, "Asian": 0.18, "Hispanic or Latino": 0.15,
                 "Black or African American": 0.10, "Two or More Races": 0.08,
                 "Not Specified": 0.04},
        "multiplier": 0.96, "ttf_range": (40, 50),
    },
    "Product": {
        "code": "PRO", "active": 100, "terminated_voluntary": 30, "rif": 10,
        "sub_depts": {
            "Product Management": 35, "Design": 25, "UX Research": 15,
            "Product Analytics": 15, "Product Ops": 10,
        },
        "level_dist": {"P1-P3": 0.35, "P4-P6": 0.28, "M1-M2": 0.22, "M3-M4": 0.12, "E1-E6": 0.03},
        "gender": {"Men": 0.50, "Women": 0.45, "Non-Binary": 0.05},
        "race": {"White": 0.38, "Asian": 0.32, "Hispanic or Latino": 0.12,
                 "Black or African American": 0.08, "Two or More Races": 0.06,
                 "Not Specified": 0.04},
        "multiplier": 1.10, "ttf_range": (40, 50),
    },
    "People": {
        "code": "PEO", "active": 80, "terminated_voluntary": 30, "rif": 12,
        "sub_depts": {
            "Recruiting": 20, "HRBPs": 15, "People Ops": 12, "L&D": 10,
            "Total Rewards": 8, "DEIB": 8, "Workplace Experience": 7,
        },
        "level_dist": {"P1-P3": 0.40, "P4-P6": 0.22, "M1-M2": 0.25, "M3-M4": 0.10, "E1-E6": 0.03},
        "gender": {"Men": 0.28, "Women": 0.68, "Non-Binary": 0.04},
        "race": {"White": 0.35, "Asian": 0.15, "Hispanic or Latino": 0.18,
                 "Black or African American": 0.20, "Two or More Races": 0.08,
                 "Not Specified": 0.04},
        "multiplier": 0.88, "ttf_range": (40, 50),
    },
    "Data & Analytics": {
        "code": "DA", "active": 55, "terminated_voluntary": 20, "rif": 0,
        "sub_depts": {
            "Data Engineering": 20, "Business Intelligence": 18, "Data Science": 17,
        },
        "level_dist": {"P1-P3": 0.38, "P4-P6": 0.30, "M1-M2": 0.22, "M3-M4": 0.08, "E1-E6": 0.02},
        "gender": {"Men": 0.55, "Women": 0.40, "Non-Binary": 0.05},
        "race": {"White": 0.32, "Asian": 0.40, "Hispanic or Latino": 0.10,
                 "Black or African American": 0.08, "Two or More Races": 0.06,
                 "Not Specified": 0.04},
        "multiplier": 1.10, "ttf_range": (45, 55),
    },
    "Executive": {
        "code": "EXE", "active": 15, "terminated_voluntary": 5, "rif": 0,
        "sub_depts": {"Office of CEO": 15},
        "level_dist": {"P1-P3": 0.30, "P4-P6": 0.00, "M1-M2": 0.00, "M3-M4": 0.00, "E1-E6": 0.70},
        "gender": {"Men": 0.40, "Women": 0.55, "Non-Binary": 0.05},
        "race": {"White": 0.40, "Asian": 0.25, "Hispanic or Latino": 0.12,
                 "Black or African American": 0.10, "Two or More Races": 0.08,
                 "Not Specified": 0.05},
        "multiplier": 1.10, "ttf_range": (45, 60),
    },
}

# ---------------------------------------------------------------------------
# Levels & bands
# ---------------------------------------------------------------------------

# Distribution of P-levels within "P1-P3" and "P4-P6" group buckets
LEVEL_GROUP_SPLITS = {
    "P1-P3": [("P1", 0.20), ("P2", 0.40), ("P3", 0.40)],
    "P4-P6": [("P4", 0.70), ("P5", 0.25), ("P6", 0.05)],
    "M1-M2": [("M1", 0.65), ("M2", 0.35)],
    "M3-M4": [("M3", 0.65), ("M4", 0.35)],
    "E1-E6": [("E1", 0.50), ("E2", 0.20), ("E3", 0.15), ("E4", 0.10), ("E5", 0.03), ("E6", 0.02)],
}

# Base bands per company profile §8b (2025 rates)
BASE_BANDS = {
    "P1": (55000, 65000, 75000),
    "P2": (72000, 85000, 98000),
    "P3": (95000, 115000, 135000),
    "P4": (125000, 150000, 175000),
    "P5": (165000, 195000, 225000),
    "P6": (200000, 240000, 280000),
    "M1": (110000, 135000, 160000),
    "M2": (140000, 170000, 200000),
    "M3": (175000, 210000, 245000),
    "M4": (210000, 260000, 310000),
    "E1": (260000, 330000, 400000),
    "E2": (310000, 390000, 470000),
    "E3": (350000, 430000, 510000),
    "E4": (380000, 470000, 560000),
    "E5": (420000, 500000, 580000),
    "E6": (450000, 550000, 650000),
}

# Levels eligible per band-bucket — used to construct comp_bands for combos
# that actually appear in the dataset.
ALLOWED_LEVELS = ["P1", "P2", "P3", "P4", "P5", "P6",
                  "M1", "M2", "M3", "M4",
                  "E1", "E2", "E3", "E4", "E5", "E6"]

# ---------------------------------------------------------------------------
# Locations (from company profile §12)
# Weights are approximate; renormalized at sample time.
# ---------------------------------------------------------------------------

LOCATIONS = [
    # (state, city, abbr, weight, zone)
    ("California", "San Francisco", "CA", 0.13, "ZONE A"),
    ("California", "Los Angeles", "CA", 0.10, "ZONE B"),
    ("California", "San Diego", "CA", 0.05, "ZONE B"),
    ("New York", "New York City", "NY", 0.10, "ZONE A"),
    ("New York", "Brooklyn", "NY", 0.04, "ZONE A"),
    ("Texas", "Austin", "TX", 0.05, "ZONE B"),
    ("Texas", "Dallas", "TX", 0.03, "ZONE B"),
    ("Texas", "Houston", "TX", 0.02, "ZONE B"),
    ("Washington", "Seattle", "WA", 0.06, "ZONE A"),
    ("Washington", "Bellevue", "WA", 0.02, "ZONE A"),
    ("Colorado", "Denver", "CO", 0.04, "ZONE B"),
    ("Colorado", "Boulder", "CO", 0.02, "ZONE B"),
    ("Illinois", "Chicago", "IL", 0.05, "ZONE B"),
    ("Massachusetts", "Boston", "MA", 0.04, "ZONE B"),
    ("Massachusetts", "Cambridge", "MA", 0.01, "ZONE B"),
    ("Georgia", "Atlanta", "GA", 0.04, "ZONE B"),
    ("North Carolina", "Raleigh", "NC", 0.02, "ZONE B"),
    ("North Carolina", "Charlotte", "NC", 0.01, "ZONE B"),
    ("Florida", "Miami", "FL", 0.02, "ZONE B"),
    ("Florida", "Tampa", "FL", 0.01, "ZONE B"),
    ("Oregon", "Portland", "OR", 0.02, "ZONE B"),
    ("Pennsylvania", "Philadelphia", "PA", 0.02, "ZONE B"),
    ("Arizona", "Phoenix", "AZ", 0.02, "ZONE B"),
    ("Minnesota", "Minneapolis", "MN", 0.02, "ZONE B"),
    ("Virginia", "Arlington", "VA", 0.02, "ZONE B"),
    ("Ohio", "Columbus", "OH", 0.02, "ZONE B"),
]

# ---------------------------------------------------------------------------
# Hire-year weights (drives synthetic hire dates; skewed to 2020-2022)
# ---------------------------------------------------------------------------

HIRE_YEAR_WEIGHTS = {
    2018: 2, 2019: 4, 2020: 12, 2021: 24, 2022: 28,
    2023: 8, 2024: 18, 2025: 4,  # 2025 is Q1 only
}

# ---------------------------------------------------------------------------
# Voluntary termination reason distribution (§11)
# ---------------------------------------------------------------------------

VOLUNTARY_REASONS = [
    ("Career Opportunities", 0.30),
    ("Compensation",          0.20),
    ("Work-Life Balance",     0.15),
    ("Manager Relationship",  0.12),
    ("Personal Reasons",      0.10),
    ("Relocation",            0.05),
    ("Return to School",      0.03),
    ("Other",                 0.05),
]

# Performance source-rating distribution (§9). Source uses 1=best, 5=worst.
SOURCE_RATING_DIST = [
    (1, 0.07),  # Outstanding                    -> target 5
    (2, 0.22),  # Exceeds Expectations           -> target 4
    (3, 0.52),  # Strong Contributor             -> target 3
    (4, 0.15),  # Partially Meets Expectations   -> target 2
    (5, 0.04),  # Does Not Meet Expectations     -> target 1
]

SOURCE_DESCRIPTIONS = {
    1: "1 - Outstanding",
    2: "2 - Exceeds Expectations",
    3: "3 - Strong Contributor",
    4: "4 - Partially Meets Expectations",
    5: "5 - Does Not Meet Expectations",
}

# Recruiting candidate source distribution (§14)
RECRUITING_ORIGIN_DIST = [
    ("applied", 0.35), ("sourced", 0.30), ("referred", 0.25),
    ("agency", 0.07), ("internal", 0.03),
]

ORIGIN_TO_CHANNEL = {
    "applied": ["Career Page", "LinkedIn", "Job Board", "Glassdoor"],
    "sourced": ["LinkedIn", "Talent Sourcer Outreach"],
    "referred": ["Referral"],
    "agency":   ["Agency"],
    "internal": ["Internal"],
}

# Performance review cycles (semi-annual)
PERF_CYCLES = [
    ("2020 Mid-Year Review Cycle", date(2020, 6, 30)),
    ("2020 Year-End Review Cycle", date(2020, 12, 31)),
    ("2021 Mid-Year Review Cycle", date(2021, 6, 30)),
    ("2021 Year-End Review Cycle", date(2021, 12, 31)),
    ("2022 Mid-Year Review Cycle", date(2022, 6, 30)),
    ("2022 Year-End Review Cycle", date(2022, 12, 31)),
    ("2023 Mid-Year Review Cycle", date(2023, 6, 30)),
    ("2023 Year-End Review Cycle", date(2023, 12, 31)),
    ("2024 Mid-Year Review Cycle", date(2024, 6, 30)),
    ("2024 Year-End Review Cycle", date(2024, 12, 31)),
]

# Engagement survey cycles (semi-annual)
EES_CYCLES = [
    ("2020 H1 Engagement Survey", date(2020, 5, 15)),
    ("2020 H2 Engagement Survey", date(2020, 11, 15)),
    ("2021 H1 Engagement Survey", date(2021, 5, 15)),
    ("2021 H2 Engagement Survey", date(2021, 11, 15)),
    ("2022 H1 Engagement Survey", date(2022, 5, 15)),
    ("2022 H2 Engagement Survey", date(2022, 11, 15)),
    ("2023 H1 Engagement Survey", date(2023, 5, 15)),
    ("2023 H2 Engagement Survey", date(2023, 11, 15)),
    ("2024 H1 Engagement Survey", date(2024, 5, 15)),
    ("2024 H2 Engagement Survey", date(2024, 11, 15)),
]

EES_THEMES = {
    "Employee Engagement": [
        ("ENG1", "I would recommend this company as a great place to work."),
        ("ENG2", "I see myself still working here two years from now."),
        ("ENG3", "I feel motivated to give my best effort every day."),
        ("ENG4", "I am proud to be part of this organization."),
    ],
    "Manager Effectiveness": [
        ("MGE1", "My manager gives me actionable feedback that helps me improve."),
        ("MGE2", "My manager genuinely cares about my career development."),
        ("MGE3", "My manager makes fair and consistent decisions."),
        ("MGE4", "I feel comfortable bringing up problems or concerns with my manager."),
    ],
    "Career Growth & Development": [
        ("CGD1", "I can see a realistic path for advancing my career here."),
        ("CGD2", "I have access to the learning opportunities I need to grow."),
        ("CGD3", "The work I do is helping me build skills that matter for my future."),
    ],
    "Work-Life Balance": [
        ("WLB1", "I can sustain my current workload without burning out."),
        ("WLB2", "I feel supported when I need to take time away from work."),
        ("WLB3", "The expectations for my availability outside of working hours are reasonable."),
    ],
    "Recognition": [
        ("REC1", "I receive meaningful recognition when I do great work."),
        ("REC2", "Recognition at this company is based on merit, not politics."),
        ("REC3", "My contributions are visible to people beyond my immediate team."),
    ],
    "Culture & Values": [
        ("CUL1", "I feel like I belong at this company."),
        ("CUL2", "The company lives its values in how decisions are actually made."),
        ("CUL3", "I trust senior leadership to act in the best interest of employees."),
    ],
    "Communication & Transparency": [
        ("COM1", "I understand how my work connects to the company's top priorities."),
        ("COM2", "Leadership communicates important changes clearly and honestly."),
        ("COM3", "Teams across the company collaborate effectively."),
    ],
    "Resources & Enablement": [
        ("RES1", "I have the tools and systems I need to do my job well."),
        ("RES2", "I have enough context and information to make good decisions in my role."),
        ("RES3", "Our team's processes help me do my best work, rather than getting in the way."),
    ],
}

ENPS_BY_DEPT = {
    "Engineering": 25, "Sales": 15, "Customer Success": 35, "Marketing": 30,
    "Product": 40, "G&A": 20, "People": 28, "Data & Analytics": 32,
    "Executive": 35,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def weighted_choice(choices_with_weights):
    """choices_with_weights: list of (item, weight). Returns one item."""
    items, weights = zip(*choices_with_weights)
    return random.choices(items, weights=weights, k=1)[0]


def weighted_choice_dict(d):
    """d: {item: weight}. Returns one item."""
    return random.choices(list(d.keys()), weights=list(d.values()), k=1)[0]


def fmt_date(d):
    if d is None:
        return ""
    return d.strftime("%m/%d/%Y")


def random_date(start, end):
    """Inclusive random date between start and end."""
    span = (end - start).days
    if span < 0:
        return start
    return start + timedelta(days=random.randint(0, span))


def hire_date_for_year(year):
    """Random date within a hire year, capped at DATA_END for 2025."""
    if year == 2025:
        return random_date(date(2025, 1, 1), DATA_END)
    return random_date(date(year, 1, 1), date(year, 12, 31))


def sample_hire_year():
    return weighted_choice(list(HIRE_YEAR_WEIGHTS.items()))


def get_title(dept, sub_dept, level):
    """Map (dept, sub_dept, level) to a job title.

    IC roles vary by sub-department; manager / director / VP titles use
    sub-department or department for differentiation. Multiple combos can
    legitimately collapse to the same title — comp_bands dedupes by title.
    """
    ic_base = SUB_DEPT_IC_TITLES.get((dept, sub_dept), f"{sub_dept} Specialist")

    if level == "P1":
        return f"Associate {ic_base}"
    if level == "P2":
        return ic_base
    if level == "P3":
        return f"Senior {ic_base}"
    if level == "P4":
        return f"Staff {ic_base}"
    if level == "P5":
        return f"Principal {ic_base}"
    if level == "P6":
        return f"Distinguished {ic_base}"
    if level == "M1":
        return f"{sub_dept} Manager"
    if level == "M2":
        return f"Senior {sub_dept} Manager"
    if level == "M3":
        return f"Director, {sub_dept}"
    if level == "M4":
        return f"Senior Director, {sub_dept}"
    if level == "E1":
        return f"VP, {sub_dept}"
    if level == "E2":
        return f"SVP, {dept}"
    if level == "E3":
        return f"Chief {dept} Officer (E3)"
    if level == "E4":
        return f"Chief {dept} Officer"
    if level == "E5":
        return "President"
    if level == "E6":
        return "Chief Executive Officer"
    return f"{sub_dept} Specialist"


SUB_DEPT_IC_TITLES = {
    ("Engineering", "Platform"):              "Software Engineer",
    ("Engineering", "AI/ML"):                 "AI/ML Engineer",
    ("Engineering", "Infrastructure"):        "Infrastructure Engineer",
    ("Engineering", "Data Engineering"):      "Data Engineer",
    ("Engineering", "Security"):              "Security Engineer",
    ("Engineering", "QA"):                    "QA Engineer",
    ("Engineering", "Developer Experience"):  "DevEx Engineer",
    ("Engineering", "DevOps & SRE"):          "Site Reliability Engineer",
    ("Sales", "SDR"):                         "Sales Development Representative",
    ("Sales", "Account Executive"):           "Account Executive",
    ("Sales", "Account Management"):          "Account Manager",
    ("Sales", "Sales Engineering"):           "Sales Engineer",
    ("Sales", "Enterprise Sales"):            "Enterprise Account Executive",
    ("Sales", "Revenue Operations"):          "Revenue Operations Analyst",
    ("Customer Success", "CSM"):              "Customer Success Manager",
    ("Customer Success", "Support"):          "Support Specialist",
    ("Customer Success", "Implementation"):   "Implementation Specialist",
    ("Customer Success", "Customer Education"): "Customer Education Specialist",
    ("G&A", "Finance"):                       "Financial Analyst",
    ("G&A", "IT"):                            "IT Specialist",
    ("G&A", "Legal"):                         "Legal Counsel",
    ("G&A", "Business Operations"):           "Business Operations Analyst",
    ("G&A", "Facilities"):                    "Facilities Coordinator",
    ("G&A", "Accounting"):                    "Accountant",
    ("Marketing", "Growth / Demand Gen"):     "Growth Marketing Specialist",
    ("Marketing", "Content"):                 "Content Marketing Specialist",
    ("Marketing", "Product Marketing"):       "Product Marketing Specialist",
    ("Marketing", "Brand & Creative"):        "Brand Designer",
    ("Marketing", "Events & Comms"):          "Events Coordinator",
    ("Marketing", "Growth Analytics"):        "Marketing Analyst",
    ("Product", "Product Management"):        "Product Manager",
    ("Product", "Design"):                    "Product Designer",
    ("Product", "UX Research"):               "UX Researcher",
    ("Product", "Product Analytics"):         "Product Analyst",
    ("Product", "Product Ops"):               "Product Ops Specialist",
    ("People", "Recruiting"):                 "Recruiter",
    ("People", "HRBPs"):                      "HR Business Partner",
    ("People", "People Ops"):                 "People Operations Specialist",
    ("People", "L&D"):                        "Learning & Development Specialist",
    ("People", "Total Rewards"):              "Compensation Analyst",
    ("People", "DEIB"):                       "DEIB Specialist",
    ("People", "Workplace Experience"):       "Workplace Experience Coordinator",
    ("Data & Analytics", "Data Engineering"): "Data Engineer",
    ("Data & Analytics", "Business Intelligence"): "BI Analyst",
    ("Data & Analytics", "Data Science"):     "Data Scientist",
    ("Executive", "Office of CEO"):           "Strategy & Operations Analyst",
}


def write_csv(filename, rows, fieldnames):
    path = SEEDS_DIR / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    return path


# ---------------------------------------------------------------------------
# 1) Comp bands
# ---------------------------------------------------------------------------

def generate_comp_bands():
    """One row per unique job title across all (dept, sub_dept, level) combos
    likely to appear in raw_employees. Zone A bands at department multiplier;
    Zone B at 95% of Zone A.
    """
    seen = {}
    rows = []
    for dept_name, dept in DEPARTMENTS.items():
        mult = dept["multiplier"]
        for sub_dept in dept["sub_depts"]:
            for level in ALLOWED_LEVELS:
                # Keep band catalog tight — only common levels per dept
                if level in ("E5", "E6") and dept_name != "Executive":
                    continue
                title = get_title(dept_name, sub_dept, level)
                if title in seen:
                    continue
                seen[title] = True

                base_min, base_mid, base_max = BASE_BANDS[level]
                za_min = round(base_min * mult)
                za_mid = round(base_mid * mult)
                za_max = round(base_max * mult)
                zb_min = round(za_min * 0.95)
                zb_mid = round(za_mid * 0.95)
                zb_max = round(za_max * 0.95)

                rows.append({
                    "Title": title,
                    "Department": dept_name,
                    "Job_Family": sub_dept,
                    "Job_Code": f"JK{len(rows) + 1:04d}",
                    "Level": level,
                    "Zone_A_Min_Salary": za_min,
                    "Zone_A_Mid_Salary": za_mid,
                    "Zone_A_Max_Salary": za_max,
                    "Zone_B_Min_Salary": zb_min,
                    "Zone_B_Mid_Salary": zb_mid,
                    "Zone_B_Max_Salary": zb_max,
                })
    return rows


# ---------------------------------------------------------------------------
# 2) Employees (the heaviest function)
# ---------------------------------------------------------------------------

def assign_levels(dept_info, total_count):
    """Sample levels for `total_count` employees per department's level
    distribution. Returns a list of level codes."""
    # Convert level-group dist → individual level codes via LEVEL_GROUP_SPLITS
    levels = []
    counts_by_group = {}
    remaining = total_count
    for lg, pct in dept_info["level_dist"].items():
        c = round(total_count * pct)
        counts_by_group[lg] = c
    # Reconcile rounding
    total_assigned = sum(counts_by_group.values())
    drift = total_count - total_assigned
    # Add/remove drift to/from largest group
    largest = max(counts_by_group, key=counts_by_group.get)
    counts_by_group[largest] += drift

    for lg, count in counts_by_group.items():
        splits = LEVEL_GROUP_SPLITS[lg]
        for _ in range(count):
            level = weighted_choice(splits)
            levels.append(level)
    return levels


def assign_sub_dept(dept_info, count):
    """Round-robin sub-department assignment proportional to active counts."""
    sub_dept_counts = dept_info["sub_depts"]
    total = sum(sub_dept_counts.values())
    pool = []
    for sd, sd_count in sub_dept_counts.items():
        pool.extend([sd] * round(count * sd_count / total))
    # Pad/trim to exact count
    while len(pool) < count:
        pool.append(random.choice(list(sub_dept_counts)))
    pool = pool[:count]
    random.shuffle(pool)
    return pool


def make_email(first, last, used):
    base = f"{first[0].lower()}{last.lower().replace(' ', '').replace('-', '').replace('.', '')}"
    base = "".join(c for c in base if c.isalnum())
    email = f"{base}@justkaizen.com"
    suffix = 2
    while email in used:
        email = f"{base}{suffix}@justkaizen.com"
        suffix += 1
    used.add(email)
    return email


def generate_employees(comp_bands):
    """Generate ~1,900 employees with full attributes, hierarchy, and
    salaries within band. Returns the list of dicts.
    """
    # Look up bands by (title, zone)
    band_by_title = {row["Title"]: row for row in comp_bands}

    employees = []
    used_emails = set()
    pos_counter = 100000
    req_counter_per_dept = defaultdict(int)

    # Cumulative termination indices to schedule RIF in Q1 2023
    rif_quota_by_dept = {d: info["rif"] for d, info in DEPARTMENTS.items()}
    voluntary_quota_by_dept = {d: info["terminated_voluntary"] for d, info in DEPARTMENTS.items()}

    # First pass: generate per-department actives + terminated
    for dept_name, info in DEPARTMENTS.items():
        active_count = info["active"]
        term_voluntary = info["terminated_voluntary"]
        term_rif = info["rif"]
        total_for_dept = active_count + term_voluntary + term_rif

        levels_pool = assign_levels(info, total_for_dept)
        sub_depts_pool = assign_sub_dept(info, total_for_dept)

        # Decide which indexes are terminated / RIF
        idx_pool = list(range(total_for_dept))
        random.shuffle(idx_pool)
        rif_idx = set(idx_pool[:term_rif])
        voluntary_idx = set(idx_pool[term_rif:term_rif + term_voluntary])

        for i in range(total_for_dept):
            level = levels_pool[i]
            sub_dept = sub_depts_pool[i]
            title = get_title(dept_name, sub_dept, level)

            # Hire date — RIFs concentrated in 2020-2022 (would have been
            # mass-hired during hypergrowth); voluntary spread broadly;
            # actives uniform-weighted with profile distribution.
            if i in rif_idx:
                hire_year = random.choices(
                    [2020, 2021, 2022], weights=[1, 3, 2]
                )[0]
            elif i in voluntary_idx:
                hire_year = sample_hire_year()
            else:
                hire_year = sample_hire_year()
            hire_date = hire_date_for_year(hire_year)

            # Names + email
            gender = weighted_choice_dict(info["gender"])
            if gender == "Women":
                first_name = fake.first_name_female()
            elif gender == "Men":
                first_name = fake.first_name_male()
            else:
                first_name = fake.first_name()
            last_name = fake.last_name()
            email = make_email(first_name, last_name, used_emails)

            # Position + Requisition
            pos_counter += 1
            position_id = f"JK{pos_counter}"
            req_counter_per_dept[dept_name] += 1
            req_id = (
                f"R-{info['code']}-1001-JustKaizen-"
                f"{req_counter_per_dept[dept_name]:04d}"
            )

            # Location & zone
            loc = weighted_choice([(loc, loc[3]) for loc in LOCATIONS])
            state, city, abbr, _, zone = loc

            race = weighted_choice_dict(info["race"])

            # Termination
            term_date = None
            term_reason = None
            term_voluntary = None
            term_regrettable = None
            employment_status = "Active"

            if i in rif_idx:
                term_date = random_date(date(2023, 1, 15), date(2023, 3, 31))
                if term_date < hire_date + timedelta(days=14):
                    # Push hire earlier so termination follows employment
                    hire_date = term_date - timedelta(days=180)
                term_reason = "Reduction in Force"
                term_voluntary = False
                term_regrettable = "Nonregrettable"
                employment_status = "Terminated"
            elif i in voluntary_idx:
                # Sample tenure between 30 and (data_end - hire_date) days
                max_tenure_days = max(
                    60, (DATA_END - hire_date).days
                )
                tenure_days = random.randint(60, max_tenure_days)
                term_date = hire_date + timedelta(days=tenure_days)
                if term_date > DATA_END:
                    term_date = DATA_END - timedelta(days=random.randint(1, 30))
                # Avoid Q1 2023 collisions with RIF (visually messy)
                if date(2023, 1, 1) <= term_date <= date(2023, 3, 31):
                    term_date = term_date + timedelta(days=90)
                    if term_date > DATA_END:
                        term_date = date(2023, 4, 15)
                term_reason = weighted_choice(VOLUNTARY_REASONS)
                term_voluntary = True
                # ~60% of voluntary are regrettable (top-performer-ish)
                term_regrettable = "Regrettable" if random.random() < 0.6 else "Nonregrettable"
                employment_status = "Terminated"

            # Comp band lookup
            band_row = band_by_title.get(title)
            if band_row is None:
                # Defensive fallback (shouldn't happen)
                continue
            if zone == "ZONE A":
                b_min = band_row["Zone_A_Min_Salary"]
                b_mid = band_row["Zone_A_Mid_Salary"]
                b_max = band_row["Zone_A_Max_Salary"]
            else:
                b_min = band_row["Zone_B_Min_Salary"]
                b_mid = band_row["Zone_B_Mid_Salary"]
                b_max = band_row["Zone_B_Max_Salary"]
            # Triangular distribution biased toward mid; clamp into band
            salary = round(random.triangular(b_min, b_max, b_mid))
            salary = max(b_min, min(b_max, salary))

            # Tenure bucket (recomputed in dbt; pre-populated as string here)
            if term_date:
                ref_date = term_date
            else:
                ref_date = REPORT_DATE
            tenure_months = (ref_date.year - hire_date.year) * 12 + (ref_date.month - hire_date.month)
            if tenure_months <= 6:
                tenure_bucket = "0-6 Months"
            elif tenure_months <= 12:
                tenure_bucket = "6-12 Months"
            elif tenure_months <= 24:
                tenure_bucket = "1-2 Years"
            elif tenure_months <= 36:
                tenure_bucket = "2-3 Years"
            elif tenure_months <= 60:
                tenure_bucket = "3-5 Years"
            else:
                tenure_bucket = "5+ Years"

            critical_talent = level in ("P5", "P6", "M3", "M4") and random.random() < 0.35
            critical_talent = critical_talent or (level.startswith("E") and random.random() < 0.7)

            employees.append({
                "Work_Email": email,
                "Requisition_ID": req_id,
                "Position_ID": position_id,
                "Report_Date": fmt_date(REPORT_DATE),
                "Employment_Status": employment_status,
                "Full_Name": f"{first_name} {last_name}",
                "First_Name": first_name,
                "Last_Name": last_name,
                "Hire_Date": fmt_date(hire_date),
                "Work_Country": "UNITED STATES",
                "Work_City": city,
                "Work_State": abbr,
                "Pay_Zone": zone,
                "Gender": gender,
                "Race": race,
                # Manager_Email + Manage_Name filled in after hierarchy build
                "Manager_Email": "",
                "Manage_Name": "",
                "Department": dept_name,
                "Sub_Department": sub_dept,
                "Team": f"{info['code']} - {sub_dept}",
                "Job_Title": title,
                "Job_Level": level,
                "Employment_Type": "Full Time",
                "Termination_Date": fmt_date(term_date) if term_date else "",
                "Termination_Reason": term_reason or "",
                "Termination_Regrettable": term_regrettable or "",
                "Termination_Voluntary": "" if term_voluntary is None else str(term_voluntary).upper(),
                "No_Direct_Reports": 0,   # set after hierarchy
                "No_Indirect_Reports": 0, # set after hierarchy
                "Manager_Status": "FALSE",  # set after hierarchy
                "Tenure_bucket": tenure_bucket,
                "Critical_Talent": "TRUE" if critical_talent else "FALSE",
                "Hire_Origin": "",  # ATS field, populated for some
                "Hire_Recruiter": "",  # set when recruiting generated
                "Salary": salary,
                # Internal columns dropped before write
                "_hire_date": hire_date,
                "_term_date": term_date,
                "_level": level,
                "_dept": dept_name,
                "_sub_dept": sub_dept,
                "_zone": zone,
            })

    # Build org hierarchy: each lower level reports to a higher level in the
    # same department. CEO has no manager.
    by_level = defaultdict(list)
    for emp in employees:
        by_level[(emp["Department"], emp["_level"])].append(emp)

    LEVEL_RANK = {l: i for i, l in enumerate(ALLOWED_LEVELS)}

    # Identify CEO: pick the executive with the highest level (E6 > E5 > ...)
    exec_employees = [e for e in employees if e["Department"] == "Executive"]
    ceo = None
    for level in ["E6", "E5", "E4", "E3", "E2", "E1"]:
        candidates = [e for e in exec_employees if e["_level"] == level
                      and e["Employment_Status"] == "Active"]
        if candidates:
            ceo = candidates[0]
            ceo["_is_ceo"] = True
            break
    if ceo is None and exec_employees:
        ceo = exec_employees[0]
        ceo["_is_ceo"] = True

    # Assign managers
    for emp in employees:
        if emp.get("_is_ceo"):
            emp["Manager_Email"] = ""
            emp["Manage_Name"] = ""
            continue
        emp_level_rank = LEVEL_RANK[emp["_level"]]
        # Look upward through level ranks to find an active candidate manager
        candidate_managers = []
        for higher_level in ALLOWED_LEVELS[emp_level_rank + 1:]:
            in_dept = by_level.get((emp["Department"], higher_level), [])
            candidate_managers = [m for m in in_dept
                                  if m["Employment_Status"] == "Active"
                                  and m is not emp]
            if candidate_managers:
                break
        # Cross-department fallback if no in-dept manager found
        if not candidate_managers:
            for higher_level in ALLOWED_LEVELS[emp_level_rank + 1:]:
                pool = []
                for d in DEPARTMENTS:
                    pool.extend(by_level.get((d, higher_level), []))
                candidate_managers = [m for m in pool
                                      if m["Employment_Status"] == "Active"
                                      and m is not emp]
                if candidate_managers:
                    break
        # Final fallback: report to CEO
        if not candidate_managers and ceo is not None:
            candidate_managers = [ceo]

        manager = random.choice(candidate_managers)
        emp["Manager_Email"] = manager["Work_Email"]
        emp["Manage_Name"] = manager["Full_Name"]

    # Compute no_direct_reports and manager_status from the hierarchy
    direct_reports = Counter()
    for emp in employees:
        if emp["Manager_Email"]:
            direct_reports[emp["Manager_Email"]] += 1
    for emp in employees:
        ndr = direct_reports.get(emp["Work_Email"], 0)
        emp["No_Direct_Reports"] = ndr
        emp["Manager_Status"] = "TRUE" if (ndr > 0 or emp["_level"][0] in ("M", "E")) else "FALSE"

    # Indirect reports: count of reports of reports
    employee_by_email = {e["Work_Email"]: e for e in employees}
    reports_of = defaultdict(list)
    for emp in employees:
        if emp["Manager_Email"]:
            reports_of[emp["Manager_Email"]].append(emp["Work_Email"])
    for emp in employees:
        # BFS skip-level
        seen, queue, indirect = set(), list(reports_of.get(emp["Work_Email"], [])), 0
        while queue:
            r_email = queue.pop()
            if r_email in seen:
                continue
            seen.add(r_email)
            sub_reports = reports_of.get(r_email, [])
            indirect += len(sub_reports)
            queue.extend(sub_reports)
        emp["No_Indirect_Reports"] = indirect

    return employees


# ---------------------------------------------------------------------------
# 3) Job history (Hire + Promotion events)
# ---------------------------------------------------------------------------

def generate_job_history(employees):
    """One Hire row per employee plus 0-2 Promotion rows for senior employees.

    Promotion events are synthetic: for employees at P4+ or M2+, inject 1-2
    earlier promotions to lower-level versions of their current title.
    """
    rows = []
    LEVEL_DOWNGRADE = {
        "P2": "P1", "P3": "P2", "P4": "P3", "P5": "P4", "P6": "P5",
        "M1": "P3", "M2": "M1", "M3": "M2", "M4": "M3",
        "E1": "M4", "E2": "E1", "E3": "E2", "E4": "E3", "E5": "E4", "E6": "E5",
    }

    for emp in employees:
        hire_date = emp["_hire_date"]
        # Hire event
        rows.append({
            "employee_id": emp["Work_Email"],
            "effective_date": fmt_date(hire_date),
            "change_type": "Hire",
            "old_department": "",
            "new_department": emp["Department"],
            "old_sub_department": "",
            "new_sub_department": emp["Sub_Department"],
            "old_job_level": "",
            "new_job_level": emp["_level"],
            "old_job_title": "",
            "new_job_title": emp["Job_Title"],
            "old_manager_id": "",
            "new_manager_id": emp["Manager_Email"],
        })

        # Promotion events for senior employees
        cur_level = emp["_level"]
        if cur_level not in LEVEL_DOWNGRADE:
            continue  # P1 has no possible prior level
        n_promos = 0
        if cur_level in ("P2", "P3", "M1"):
            n_promos = random.choice([0, 0, 1])
        elif cur_level in ("P4", "P5", "P6", "M2", "M3", "M4"):
            n_promos = random.choice([0, 1, 1, 2])
        elif cur_level in ("E1", "E2", "E3", "E4"):
            n_promos = random.choice([1, 1, 2])
        elif cur_level in ("E5", "E6"):
            n_promos = random.choice([2, 3])

        # Generate sequential promotions ending at cur_level
        if n_promos > 0:
            chain = [cur_level]
            for _ in range(n_promos):
                prev = LEVEL_DOWNGRADE.get(chain[-1])
                if not prev:
                    break
                chain.append(prev)
            chain.reverse()  # earliest level first
            # Promotion dates between hire_date and (term_date or REPORT_DATE)
            end_anchor = emp["_term_date"] or REPORT_DATE
            span_days = (end_anchor - hire_date).days
            if span_days < 365:
                continue
            for idx in range(len(chain) - 1):
                old_lvl = chain[idx]
                new_lvl = chain[idx + 1]
                # Spread promotions evenly with jitter
                pct = (idx + 1) / len(chain)
                eff = hire_date + timedelta(days=int(span_days * pct))
                eff += timedelta(days=random.randint(-30, 30))
                if eff <= hire_date or eff >= end_anchor:
                    continue
                rows.append({
                    "employee_id": emp["Work_Email"],
                    "effective_date": fmt_date(eff),
                    "change_type": "Promotion",
                    "old_department": emp["Department"],
                    "new_department": emp["Department"],
                    "old_sub_department": emp["Sub_Department"],
                    "new_sub_department": emp["Sub_Department"],
                    "old_job_level": old_lvl,
                    "new_job_level": new_lvl,
                    "old_job_title": get_title(emp["Department"], emp["Sub_Department"], old_lvl),
                    "new_job_title": get_title(emp["Department"], emp["Sub_Department"], new_lvl),
                    "old_manager_id": emp["Manager_Email"],
                    "new_manager_id": emp["Manager_Email"],
                })
    return rows


# ---------------------------------------------------------------------------
# 4) Performance reviews (manager Performance Category only)
# ---------------------------------------------------------------------------

def generate_performance(employees):
    """One row per (active employee × cycle after their hire date + 6mo).

    Only emits manager + Performance Category rows — what stg_performance
    filters to. Includes inverted source format (1 = best, 5 = Partially
    Meets per JustKaizen extension).
    """
    rows = []
    employee_by_email = {e["Work_Email"]: e for e in employees}

    for cycle_name, cycle_end in PERF_CYCLES:
        for emp in employees:
            hire = emp["_hire_date"]
            term = emp["_term_date"]
            # Eligibility per user spec: any employee hired before cycle end
            # gets a review, including those who terminated within the cycle
            # period (they had partial-period output to review). Drop only
            # those terminated well before the cycle started.
            if hire >= cycle_end:
                continue
            if term and term < cycle_end - timedelta(days=120):
                continue
            score = weighted_choice(SOURCE_RATING_DIST)

            reviewer_email = emp["Manager_Email"] or ""
            if reviewer_email and reviewer_email in employee_by_email:
                reviewer_name = employee_by_email[reviewer_email]["Full_Name"]
            else:
                reviewer_name = ""

            qid = f"Q-PERF-CAT-{emp['Work_Email']}-{cycle_name}"
            qid_hash = hashlib.md5(qid.encode()).hexdigest()[:12]

            rows.append({
                "Question_ID": f"Q{qid_hash}",
                "Reviewee_Email": emp["Work_Email"],
                "Cycle_Name": cycle_name,
                "Reviewer_Name": reviewer_name,
                "Reviewer_email": reviewer_email,
                "Reviewee_Name": emp["Full_Name"],
                "Question": "Performance Category",
                "Score": str(score),
                "Score_Description": SOURCE_DESCRIPTIONS[score],
                "Calibrated_Score": " --",
                "Calibrated_Score_Description": " --",
                "Response_Text": " --",
                "Response_Type": "manager",
            })
    return rows


# ---------------------------------------------------------------------------
# 5) Recruiting (one row per candidate, ~3 per req: 1 hired + ~2 rejected)
# ---------------------------------------------------------------------------

def generate_recruiting(employees):
    """One row per candidate. Every employee's req gets a hired row plus
    1-3 archived candidates. Time-to-fill follows department targets;
    offer-acceptance ~80%."""
    rows = []
    candidate_counter = 0

    for emp in employees:
        dept = emp["Department"]
        info = DEPARTMENTS[dept]
        ttf_lo, ttf_hi = info["ttf_range"]
        req_id = emp["Requisition_ID"]
        req_open = emp["_hire_date"] - timedelta(days=random.randint(ttf_lo, ttf_hi))

        origin = weighted_choice(RECRUITING_ORIGIN_DIST)
        channel = random.choice(ORIGIN_TO_CHANNEL[origin])

        application_date = req_open + timedelta(days=random.randint(1, 14))
        s1_date = application_date + timedelta(days=random.randint(7, 14))
        s2_date = s1_date + timedelta(days=random.randint(7, 14))
        offer_extended = s2_date + timedelta(days=random.randint(5, 12))
        offer_accept = offer_extended + timedelta(days=random.randint(2, 7))
        start_date = emp["_hire_date"]

        # Hired candidate row
        candidate_counter += 1
        rows.append({
            "Candidate_ID": f"CAND-{candidate_counter:06d}",
            "Requisition_ID": req_id,
            "Requisition_Fill_Start_Date": fmt_date(req_open),
            "Outcome": "Hired",
            "Job": emp["Job_Title"],
            "Job_Status": "closed",
            "Recruiter": fake.name(),
            "Hiring_Manager": emp["Manage_Name"] or "TBD",
            "Origin": origin,
            "Source": channel,
            "Current_Interview_Stage": "Hired",
            "Furthest_Stage_Reached": "Hired",
            "Archive_Reason": "",
            "Offer_Decline_Category": "",
            "Candidate_Application_Date": fmt_date(application_date),
            "Candidate_Stage_1_Interview_Date": fmt_date(s1_date),
            "Candidate_Stage_2_Interview_Date": fmt_date(s2_date),
            "Candidate_Offer_Stage_Entered_Date": fmt_date(offer_extended),
            "Candidate_Offer_Accept_Date": fmt_date(offer_accept),
            "Candidate_Start_Date": fmt_date(start_date),
        })

        # Archived candidates for the same req (1-3, mode 2)
        n_archived = random.choices([1, 2, 2, 3], k=1)[0]
        for _ in range(n_archived):
            candidate_counter += 1
            arch_origin = weighted_choice(RECRUITING_ORIGIN_DIST)
            arch_channel = random.choice(ORIGIN_TO_CHANNEL[arch_origin])
            arch_app_date = req_open + timedelta(days=random.randint(1, 30))
            # Stage they reached
            stage_pool = [
                ("Applied",          "Not qualified",        None,    None,    None),
                ("Recruiter Screen", "Not qualified",        7,       None,    None),
                ("Phone Screen",     "Not qualified",        7,       14,      None),
                ("Onsite",           "Position filled",      7,       14,      21),
                ("Offer",            "Offer declined",       7,       14,      21),
            ]
            stage, archive_reason, s1_off, s2_off, offer_off = random.choice(stage_pool)
            s1_d = arch_app_date + timedelta(days=s1_off) if s1_off else None
            s2_d = arch_app_date + timedelta(days=s2_off) if s2_off else None
            offer_d = arch_app_date + timedelta(days=offer_off) if offer_off else None
            # Offer-decline category if they declined
            decline_cat = ""
            if stage == "Offer":
                decline_cat = random.choice(
                    ["Compensation", "Accepted Other Offer", "Location",
                     "Career Growth", "Personal"]
                )
            rows.append({
                "Candidate_ID": f"CAND-{candidate_counter:06d}",
                "Requisition_ID": req_id,
                "Requisition_Fill_Start_Date": fmt_date(req_open),
                "Outcome": "Archived",
                "Job": emp["Job_Title"],
                "Job_Status": "closed",
                "Recruiter": fake.name(),
                "Hiring_Manager": emp["Manage_Name"] or "TBD",
                "Origin": arch_origin,
                "Source": arch_channel,
                "Current_Interview_Stage": "Archived",
                "Furthest_Stage_Reached": stage,
                "Archive_Reason": archive_reason,
                "Offer_Decline_Category": decline_cat,
                "Candidate_Application_Date": fmt_date(arch_app_date),
                "Candidate_Stage_1_Interview_Date": fmt_date(s1_d) if s1_d else "",
                "Candidate_Stage_2_Interview_Date": fmt_date(s2_d) if s2_d else "",
                "Candidate_Offer_Stage_Entered_Date": fmt_date(offer_d) if offer_d else "",
                "Candidate_Offer_Accept_Date": "",
                "Candidate_Start_Date": "",
            })
    return rows


# ---------------------------------------------------------------------------
# 6) Engagement responses (anonymized)
# ---------------------------------------------------------------------------

def _theme_baseline_score(theme, dept):
    """Slightly tilt theme baseline scores by department for realism."""
    dept_offsets = {
        "Engineering": 0.1, "Sales": -0.1, "Customer Success": 0.15,
        "Marketing": 0.1, "Product": 0.2, "G&A": 0.0, "People": 0.05,
        "Data & Analytics": 0.1, "Executive": 0.15,
    }
    theme_baseline = {
        "Employee Engagement": 3.8, "Manager Effectiveness": 3.7,
        "Career Growth & Development": 3.5, "Work-Life Balance": 3.6,
        "Recognition": 3.5, "Culture & Values": 3.7,
        "Communication & Transparency": 3.5, "Resources & Enablement": 3.6,
    }
    return theme_baseline[theme] + dept_offsets.get(dept, 0)


def _likert_sample(mean):
    """Sample 1-5 likert biased toward `mean` (truncated normal)."""
    val = int(round(np.clip(np.random.normal(mean, 0.9), 1, 5)))
    return val


def _enps_sample(dept_enps):
    """Sample a 0-10 eNPS score skewed to land near the dept's enps mean.

    dept_enps is target enps (-100..100). Each individual scores 0-10.
    Promoters 9-10, Passives 7-8, Detractors 0-6.
    Approximate by inverse logit.
    """
    target_promoter_pct = 0.50 + dept_enps / 200  # crude mapping
    target_detractor_pct = 0.20 - dept_enps / 400
    r = random.random()
    if r < target_promoter_pct:
        return random.randint(9, 10)
    elif r < target_promoter_pct + 0.30:
        return random.randint(7, 8)
    else:
        return random.randint(0, 6)


def generate_engagement(employees):
    """One row per (anonymized respondent × question × cycle).

    Persistent anonymized IDs per employee (so the same person across cycles
    keeps the same hash, enabling longitudinal slicing in dbt while keeping
    the link severed from raw_employees.Work_Email).
    """
    rows = []
    # Persistent anonymous ID per employee
    anon_id_by_employee = {
        e["Work_Email"]: f"USR-{hashlib.sha256(e['Work_Email'].encode()).hexdigest()[:12]}"
        for e in employees
    }

    for cycle_name, cycle_date in EES_CYCLES:
        # Pool of eligible respondents: hired before the cycle and active or
        # recently terminated.
        eligible = [
            e for e in employees
            if e["_hire_date"] <= cycle_date
            and (e["_term_date"] is None or e["_term_date"] >= cycle_date)
        ]
        # Response rate 70-85% of eligible (varies per cycle)
        rate = random.uniform(0.70, 0.85)
        n_respondents = int(round(len(eligible) * rate))
        respondents = random.sample(eligible, n_respondents) if n_respondents <= len(eligible) else eligible

        for emp in respondents:
            anon = anon_id_by_employee[emp["Work_Email"]]
            for theme, questions in EES_THEMES.items():
                baseline = _theme_baseline_score(theme, emp["Department"])
                for qcode, qtext in questions:
                    if qcode == "ENG1":
                        # eNPS-flavored question: 0-10 scale instead of 1-5
                        enps_val = _enps_sample(ENPS_BY_DEPT.get(emp["Department"], 25))
                        if enps_val >= 9:
                            cat = "Promoter"
                        elif enps_val >= 7:
                            cat = "Passive"
                        else:
                            cat = "Detractor"
                        rows.append({
                            "Anonymized_User_ID": anon,
                            "EES_Cycle": cycle_name,
                            "EES_Submission_Date": fmt_date(cycle_date),
                            "EES_Theme_Name": theme,
                            "EES_Question": qtext,
                            "eNPS": enps_val,
                            "enps_Category": cat,
                            "Response_Likert": "",
                            "Department": emp["Department"],
                            "Sub_Department": emp["Sub_Department"],
                            "Tenure_Bucket": emp["Tenure_bucket"],
                            "Team": emp["Team"],
                            "Radford_Level": emp["Job_Level"],
                            "Is_A_Manager": emp["Manager_Status"],
                            "Is_Top_Performer": emp["Critical_Talent"],
                        })
                    else:
                        likert = _likert_sample(baseline)
                        rows.append({
                            "Anonymized_User_ID": anon,
                            "EES_Cycle": cycle_name,
                            "EES_Submission_Date": fmt_date(cycle_date),
                            "EES_Theme_Name": theme,
                            "EES_Question": qtext,
                            "eNPS": "",
                            "enps_Category": "",
                            "Response_Likert": likert,
                            "Department": emp["Department"],
                            "Sub_Department": emp["Sub_Department"],
                            "Tenure_Bucket": emp["Tenure_bucket"],
                            "Team": emp["Team"],
                            "Radford_Level": emp["Job_Level"],
                            "Is_A_Manager": emp["Manager_Status"],
                            "Is_Top_Performer": emp["Critical_Talent"],
                        })
    return rows


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate(employees, comp_bands, job_history, performance, recruiting, engagement):
    print("\n" + "=" * 60)
    print("Validation")
    print("=" * 60)

    actives = [e for e in employees if e["Employment_Status"] == "Active"]
    terminated = [e for e in employees if e["Employment_Status"] == "Terminated"]
    rifs = [e for e in terminated if e["Termination_Reason"] == "Reduction in Force"]

    print(f"  Active employees:           {len(actives):,} (target 1,200)")
    print(f"  Terminated employees:       {len(terminated):,} (target ~700)")
    print(f"  Total employees ever:       {len(employees):,} (target ~1,900)")
    print(f"  Q1 2023 RIF count:          {len(rifs):,} (target 150)")

    # Manager_Email integrity
    valid_emails = {e["Work_Email"] for e in employees}
    orphan_managers = [
        e for e in employees
        if e["Manager_Email"] and e["Manager_Email"] not in valid_emails
    ]
    print(f"  Orphan manager_id refs:     {len(orphan_managers):,} (target 0)")

    # Salary in band
    band_by_title = {b["Title"]: b for b in comp_bands}
    out_of_band = 0
    for e in employees:
        b = band_by_title.get(e["Job_Title"])
        if not b:
            out_of_band += 1
            continue
        if e["Pay_Zone"] == "ZONE A":
            mn, mx = b["Zone_A_Min_Salary"], b["Zone_A_Max_Salary"]
        else:
            mn, mx = b["Zone_B_Min_Salary"], b["Zone_B_Max_Salary"]
        if not (mn <= e["Salary"] <= mx):
            out_of_band += 1
    print(f"  Salary out of band:         {out_of_band:,} (target 0)")

    # Department headcount totals
    print("  Active headcount by department:")
    by_dept = Counter(e["Department"] for e in actives)
    for d, info in DEPARTMENTS.items():
        print(f"    {d:<22} {by_dept[d]:>4} (target {info['active']})")

    # Row counts for derived tables
    print(f"  Comp band rows:             {len(comp_bands):,}")
    print(f"  Job history rows:           {len(job_history):,}")
    print(f"  Performance rows:           {len(performance):,}")
    print(f"  Recruiting rows:            {len(recruiting):,}")
    print(f"  Engagement rows:            {len(engagement):,}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

EMPLOYEE_FIELDS = [
    "Work_Email", "Requisition_ID", "Position_ID", "Report_Date",
    "Employment_Status", "Full_Name", "First_Name", "Last_Name",
    "Hire_Date", "Work_Country", "Work_City", "Work_State", "Pay_Zone",
    "Gender", "Race", "Manager_Email", "Manage_Name",
    "Department", "Sub_Department", "Team", "Job_Title", "Job_Level",
    "Employment_Type", "Termination_Date", "Termination_Reason",
    "Termination_Regrettable", "Termination_Voluntary",
    "No_Direct_Reports", "No_Indirect_Reports", "Manager_Status",
    "Tenure_bucket", "Critical_Talent", "Hire_Origin", "Hire_Recruiter",
    "Salary",
]

COMP_FIELDS = [
    "Title", "Department", "Job_Family", "Job_Code", "Level",
    "Zone_A_Min_Salary", "Zone_A_Mid_Salary", "Zone_A_Max_Salary",
    "Zone_B_Min_Salary", "Zone_B_Mid_Salary", "Zone_B_Max_Salary",
]

JOB_HISTORY_FIELDS = [
    "employee_id", "effective_date", "change_type",
    "old_department", "new_department",
    "old_sub_department", "new_sub_department",
    "old_job_level", "new_job_level",
    "old_job_title", "new_job_title",
    "old_manager_id", "new_manager_id",
]

PERF_FIELDS = [
    "Question_ID", "Reviewee_Email", "Cycle_Name", "Reviewer_Name",
    "Reviewer_email", "Reviewee_Name", "Question", "Score",
    "Score_Description", "Calibrated_Score", "Calibrated_Score_Description",
    "Response_Text", "Response_Type",
]

RECRUITING_FIELDS = [
    "Candidate_ID", "Requisition_ID", "Requisition_Fill_Start_Date",
    "Outcome", "Job", "Job_Status", "Recruiter", "Hiring_Manager",
    "Origin", "Source", "Current_Interview_Stage", "Furthest_Stage_Reached",
    "Archive_Reason", "Offer_Decline_Category", "Candidate_Application_Date",
    "Candidate_Stage_1_Interview_Date", "Candidate_Stage_2_Interview_Date",
    "Candidate_Offer_Stage_Entered_Date", "Candidate_Offer_Accept_Date",
    "Candidate_Start_Date",
]

EES_FIELDS = [
    "Anonymized_User_ID", "EES_Cycle", "EES_Submission_Date",
    "EES_Theme_Name", "EES_Question", "eNPS", "enps_Category",
    "Response_Likert", "Department", "Sub_Department", "Tenure_Bucket",
    "Team", "Radford_Level", "Is_A_Manager", "Is_Top_Performer",
]


def main():
    print("=" * 60)
    print("JustKaizen AI: Synthetic Data Generation")
    print("=" * 60)
    print(f"Output dir: {SEEDS_DIR}")
    print(f"RNG seed:   {RNG_SEED}")

    print("\n[1/6] Generating comp bands...")
    comp_bands = generate_comp_bands()
    print(f"  {len(comp_bands)} bands")

    print("\n[2/6] Generating employees...")
    employees = generate_employees(comp_bands)
    print(f"  {len(employees)} employees ({sum(1 for e in employees if e['Employment_Status'] == 'Active')} active)")

    print("\n[3/6] Generating job history...")
    job_history = generate_job_history(employees)
    print(f"  {len(job_history)} events")

    print("\n[4/6] Generating performance reviews...")
    performance = generate_performance(employees)
    print(f"  {len(performance)} reviews")

    print("\n[5/6] Generating recruiting pipeline...")
    recruiting = generate_recruiting(employees)
    print(f"  {len(recruiting)} candidate records")

    print("\n[6/6] Generating engagement responses...")
    engagement = generate_engagement(employees)
    print(f"  {len(engagement)} responses")

    # Strip internal-only fields from employees before write
    employees_for_csv = []
    for e in employees:
        row = {k: v for k, v in e.items() if not k.startswith("_")}
        employees_for_csv.append(row)

    print("\nWriting CSVs...")
    write_csv("raw_comp_bands.csv",     comp_bands,           COMP_FIELDS)
    write_csv("raw_employees.csv",      employees_for_csv,    EMPLOYEE_FIELDS)
    write_csv("raw_job_history.csv",    job_history,          JOB_HISTORY_FIELDS)
    write_csv("raw_performance.csv",    performance,          PERF_FIELDS)
    write_csv("raw_offers_hires.csv",   recruiting,           RECRUITING_FIELDS)
    write_csv("raw_ees_responses.csv",  engagement,           EES_FIELDS)
    print("  Done.")

    validate(employees, comp_bands, job_history, performance, recruiting, engagement)


if __name__ == "__main__":
    main()
