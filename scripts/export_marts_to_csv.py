"""
Export all dbt mart tables from BigQuery to individual CSV files.

Usage:
    pip install google-cloud-bigquery
    python scripts/export_marts_to_csv.py

Output:
    Creates one CSV per mart table in exports/ folder:
      exports/fct_attrition_reporting.csv
      exports/fct_recruiting_reporting.csv
      exports/fct_workforce_composition.csv
      exports/fct_compensation_reporting.csv
      exports/fct_employee_roster.csv
      exports/fct_recruiting_velocity.csv
      exports/fct_engagement_trends.csv
      exports/fct_performance_distribution.csv

Auth:
    Uses the same service account keyfile as dbt (from ~/.dbt/profiles.yml).
"""

import os
import sys
from google.cloud import bigquery
from google.oauth2 import service_account

PROJECT_ID = "just-kaizen-ai"
DATASET = "raw_marts"
KEYFILE = os.path.expanduser("~/Documents/Claude_Code/Keys/just-kaizen-ai-6ee503c7c428.json")

# All 8 mart tables — full SELECT * with no filters.
# CSVs don't have the row/cell limits that Google Sheets does,
# so we can pull everything.
TABLES = [
    "fct_attrition_reporting",
    "fct_recruiting_reporting",
    "fct_workforce_composition",
    "fct_compensation_reporting",
    "fct_employee_roster",
    "fct_recruiting_velocity",
    "fct_engagement_trends",
    "fct_performance_distribution",
]

def main():
    # Set up output directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    export_dir = os.path.join(project_root, "exports")
    os.makedirs(export_dir, exist_ok=True)

    # Connect to BigQuery using service account key
    credentials = service_account.Credentials.from_service_account_file(KEYFILE)
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    print(f"Connected to BigQuery project: {PROJECT_ID}\n")

    total_rows = 0

    for table_name in TABLES:
        print(f"  Exporting {table_name}...", end=" ", flush=True)

        query = f"SELECT * FROM `{PROJECT_ID}.{DATASET}.{table_name}`"
        df = client.query(query).to_dataframe()

        csv_path = os.path.join(export_dir, f"{table_name}.csv")
        df.to_csv(csv_path, index=False)

        row_count = len(df)
        col_count = len(df.columns)
        total_rows += row_count
        print(f"{row_count:,} rows x {col_count} cols")

    print(f"\nDone. {total_rows:,} total rows across {len(TABLES)} tables.")
    print(f"CSVs saved to: {export_dir}/")


if __name__ == "__main__":
    main()
