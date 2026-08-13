"""
Bronze layer ingestion: bank CSV -> partitioned Parquet on S3

Design:
- One parser function per card issuer (Amex, Chase), each normalizing to a
  common internal schema before ingestion metadata is added.
- Common schema written to bronze is intentionally "raw-ish" — minimal
  transformation here. Real cleaning/typing/categorization belongs in the
  silver dbt staging models, not in bronze ingestion.
- Partitioned by source + statement month so DuckDB can glob-read cleanly:
    s3://<bucket>/bronze/bank_transactions/source=amex/year_month=2026-08/*.parquet
    s3://<bucket>/bronze/bank_transactions/source=chase/year_month=2026-08/*.parquet

Usage:
    python bronze/bronze_ingest_bank_csv.py --source amex --file ~/Downloads/amex_activity.csv
    python bronze/bronze_ingest_bank_csv.py --source chase --file ~/Downloads/Chase_Activity.CSV
"""

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path
import os
import pandas as pd
import boto3
import tempfile
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config from .env / config file
# ---------------------------------------------------------------------------
S3_BUCKET = os.environ["WEALTH_LAKEHOUSE_BUCKET"]
BRONZE_PREFIX = "bronze/bank_transactions"
AWS_REGION = os.environ["AWS_REGION"]  # e.g. "us-west-2"


def account_label_from_filename(file_path: Path) -> str:
    stem = file_path.stem  # filename without extension
    label = stem.split("_")[0]
    return label

# ---------------------------------------------------------------------------
# Per-issuer parsers
# Each returns a DataFrame with a common internal schema:
#   transaction_date (date), post_date (date or None), description (str),
#   amount (float, POSITIVE = money out / spend, NEGATIVE = credit/payment),
#   category_raw (str or None), source_account_hint (str or None)
# ---------------------------------------------------------------------------

def parse_amex(xlsx_path: Path) -> pd.DataFrame:
    """
    Amex Activity CSV export.
    TODO: confirm actual column headers against a real downloaded file —
    Amex's export columns can vary slightly by card product. Commonly:
    'Date', 'Description', 'Card Member', 'Account #', 'Amount'
    Amex convention: positive amount = charge/spend, negative = payment/credit
    (this already matches our common schema sign convention — no flip needed).
    """
    df = pd.read_excel(xlsx_path, skiprows=6)

    df = df.rename(columns={
        "Date": "transaction_date",
        "Description": "description",
        "Amount": "amount",
        "Category": "category_raw"
    })
 
    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").dt.date
    df["amount"] = df["amount"].astype(float)
    df["source_account_hint"] = account_label_from_filename(xlsx_path)

 
    # Collisions with the common schema columns once you see the real headers
    common_cols = ["transaction_date", "description", "amount", "category_raw", "source_account_hint"]    
    passthrough_cols = [c for c in df.columns if c not in common_cols and c not in
                         ["Date", "Description", "Amount", "Category"]]
 
    return df[common_cols + passthrough_cols]


def parse_chase(csv_path: Path) -> pd.DataFrame:
    """
    Chase credit card Activity CSV export.
    Columns per Chase's export: Transaction Date, Post Date, Description,
    Category, Type, Amount.

    """
    df = pd.read_csv(csv_path)

    df = df.rename(columns={
        "Transaction Date": "transaction_date",
        "Description": "description",
        "Category": "category_raw",
        "Amount": "amount"
    })

    df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").dt.date
    df["amount"] = df["amount"].astype(float) * -1  # flip sign, see docstring
    df["source_account_hint"] = account_label_from_filename(csv_path)

    cols = ["transaction_date", "description", "amount", "category_raw", "source_account_hint"]
    return df[cols]


PARSERS = {
    "amex": parse_amex,
    "chase": parse_chase,
}


# ---------------------------------------------------------------------------
# Common ingestion logic
# ---------------------------------------------------------------------------

def add_ingestion_metadata(df: pd.DataFrame, source: str, source_file: Path) -> pd.DataFrame:
    """Stamp every row with lineage metadata as a hash to dedup"""
    df = df.copy()
    df["source"] = source
    df["source_file"] = source_file.name
    df["ingested_at"] = datetime.now(timezone.utc)

    hash_cols = ["transaction_date", "description", "amount"]

    # Tie breaker implementation
    base_key = df[hash_cols].apply(lambda row: "|".join(str(v) for v in row), axis=1)
    occurrence = base_key.groupby(base_key).cumcount().astype(str)
 
    df["row_hash"] = (base_key + "|" + occurrence).apply(
        lambda s: hashlib.sha256(s.encode()).hexdigest()
    )
    return df


def add_partition_key(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """
    Adds a 'year_month' column to the DataFrame based on the transaction date.
    This allows rows within the same CSV to be split into their correct months.
    """
    # Create a copy to avoid SettingWithCopyWarning
    df = df.copy()
    
    # Ensure the date column is in datetime format
    dt = pd.to_datetime(df[date_column])
    
    # Create the year_month partition string (e.g., '2026-08')
    df['year_month'] = dt.dt.to_period('M').astype(str)
    
    return df


def write_to_bronze(df: pd.DataFrame, source: str, dry_run: bool = False) -> None:
    """Split by year_month and write one Parquet file per partition to S3."""
    df = add_partition_key(df, "transaction_date")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    for year_month, group in df.groupby("year_month"):
        local_tmp = Path(tempfile.gettempdir()) / f"{source}_{year_month}.parquet"
        group.drop(columns=["year_month"]).to_parquet(local_tmp, index=False)

        s3_key = f"{BRONZE_PREFIX}/source={source}/year_month={year_month}/{local_tmp.name}"

        if dry_run:
            print(f"[dry-run] would upload {local_tmp} -> s3://{S3_BUCKET}/{s3_key}")
            local_tmp.unlink(missing_ok=True)
            continue

        s3.upload_file(str(local_tmp), S3_BUCKET, s3_key)
        print(f"Uploaded {len(group)} rows -> s3://{S3_BUCKET}/{s3_key}")

        local_tmp.unlink()


def main():
    parser = argparse.ArgumentParser(description="Ingest a bank CSV export into the bronze layer.")
    parser.add_argument("--source", required=True, choices=PARSERS.keys())
    parser.add_argument("--file", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true", help="Parse and show summary without uploading to S3")
    args = parser.parse_args()

    args.file = args.file.expanduser()

    if not args.file.exists():
        sys.exit(f"File not found: {args.file}")

    parse_fn = PARSERS[args.source]
    df = parse_fn(args.file)

    if df is None or df.empty:
        sys.exit(f"Validation Failed: {args.file.name} is empty or returned no data.")
    
    df = add_ingestion_metadata(df, args.source, args.file)

    print(f"Parsed {len(df)} rows from {args.file.name}")
    print(df.head())

     # Check 1: Assert no null transaction dates
    null_dates = df['transaction_date'].isnull().sum()
    assert null_dates == 0, f"Validation Failed: Found {null_dates} missing transaction_date values."

    # Check 2: Ensure amount is numeric (Coerce and check for NaN)
    # This catches hidden string characters like '$' or commas before asserting
    numeric_amounts = pd.to_numeric(df['amount'], errors='coerce')
    null_amounts = numeric_amounts.isnull().sum()
    assert null_amounts == 0, f"Validation Failed: Found {null_amounts} rows where 'amount' is not numeric."

    # Check 3: Flag duplicate row_hash values within this file
    duplicate_count = df['row_hash'].duplicated().sum()
    if duplicate_count > 0:
        print(f"Warning: Found {duplicate_count} duplicate row_hash entries in this file — investigate, but continuing.")

    # Check 4: Sanity-check date range (Statement Period)
    # Convert to datetime objects for accurate timedelta math
    df_dates = pd.to_datetime(df['transaction_date']).dt.date
    min_date = df_dates.min()
    max_date = df_dates.max()
    days_span = (max_date - min_date).days

    if days_span == 0:
        print(f"Note: file only contains transactions for a single day ({min_date}).")
    elif days_span > 35:
        print(f"Warning: date range spans {days_span} days ({min_date} to {max_date}) — wider than a typical single statement period, double check {args.file.name}.")
    
    write_to_bronze(df, args.source, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
