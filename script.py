#!/usr/bin/env python3
"""
Demographics UPSERT Script
---------------------------------
• Hardcoded Google Sheet (CSV export)
• Uses u_id as uid
• Creates grad_term column (from major1_grad_term -> major2 -> major3)
• Stores full row as JSON payload
• Handles NaN -> null for JSON
• UPSERT into src.src_demographics
"""

import json
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values


# ==========================================
# GOOGLE SHEET CONFIG
# ==========================================

SPREADSHEET_ID = ""
GID = ""  # Sheet1 usually 0

CSV_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/export?format=csv&gid={GID}"


# ==========================================
# DATABASE CONFIG (AWS RDS)
# ==========================================

DB_CONFIG = {
    "host": "",
    "port": 5432,
    "dbname": "",   # MUST match actual DB
    "user": "",
    "password": ""
}

# ==========================================
# FETCH GOOGLE SHEET
# ==========================================

def fetch_google_sheet():

    print("Downloading sheet from Google...")

    df = pd.read_csv(CSV_URL)

    # Normalize column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    # ------------------------------------------
    # CREATE grad_term COLUMN
    # ------------------------------------------

    if "grad_term" not in df.columns:

        df["grad_term"] = None

        if "major1_grad_term" in df.columns:
            df["grad_term"] = df["major1_grad_term"]

        if "major2_grad_term" in df.columns:
            df["grad_term"] = df["grad_term"].fillna(df["major2_grad_term"])

        if "major3_grad_term" in df.columns:
            df["grad_term"] = df["grad_term"].fillna(df["major3_grad_term"])

    return df


# ==========================================
# UPSERT INTO POSTGRES
# ==========================================

def upsert_into_postgres(df):

    required_columns = ["u_id", "grad_term"]

    for col in required_columns:
        if col not in df.columns:
            raise Exception(
                f"Required column '{col}' not found. Found columns: {df.columns.tolist()}"
            )

    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    records = []

    for _, row in df.iterrows():

        uid = str(row["u_id"]).strip()

        term = str(row["grad_term"]).strip()

        # Convert NaN -> None (valid JSON null)
        clean_row = row.where(pd.notnull(row), None)

        row_dict = clean_row.to_dict()

        row_json = json.dumps(row_dict, sort_keys=True)

        records.append((
            uid,
            term,
            row_json,
            SPREADSHEET_ID
        ))

    upsert_query = """
        INSERT INTO src.src_demographics
        (uid, term, payload, source_file)
        VALUES %s
        ON CONFLICT (uid, term)
        DO UPDATE SET
            payload = EXCLUDED.payload,
            source_file = EXCLUDED.source_file,
            ingested_at = NOW();
    """

    execute_values(cursor, upsert_query, records)

    conn.commit()
    cursor.close()
    conn.close()


# ==========================================
# MAIN
# ==========================================

def main():

    df = fetch_google_sheet()

    if df.empty:
        print("No data found in sheet.")
        return

    print(f"Processing {len(df)} rows...")

    upsert_into_postgres(df)

    print("Demographics upsert completed successfully.")


if __name__ == "__main__":
    main()