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
    python bronze_ingest_bank_csv.py --source amex --file ~/Downloads/amex_activity.csv
    python bronze_ingest_bank_csv.py --source chase --file ~/Downloads/Chase_Activity.CSV
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
S3_BUCKET = os.environ["WEALTH_LAKEHOUSE_BUCKET"]  # TODO: load from env var, e.g. os.environ["WEALTH_LAKEHOUSE_BUCKET"]
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
    df["post_date"] = pd.to_datetime(df["post_date"], errors="coerce").dt.date
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
    df["row_hash"] = df[hash_cols].apply(
        lambda row: hashlib.sha256("|".join(str(v) for v in row).encode()).hexdigest(),
        axis=1,
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
            continue

        # TODO: decide overwrite vs. append behavior. As written, this OVERWRITES
        # any existing file at this exact key. If you re-run ingestion for a file
        # you've already loaded, you'll clobber, not duplicate — that's probably
        # what you want, but consider a more robust upsert/merge strategy once
        # you're past the prototype stage (e.g. content-addressed filenames using
        # row_hash, or a proper merge in silver using row_hash for dedup instead).
        s3.upload_file(str(local_tmp), S3_BUCKET, s3_key)
        print(f"Uploaded {len(group)} rows -> s3://{S3_BUCKET}/{s3_key}")

        local_tmp.unlink()


def main():
    parser = argparse.ArgumentParser(description="Ingest a bank CSV export into the bronze layer.")
    parser.add_argument("--source", required=True, choices=PARSERS.keys())
    parser.add_argument("--file", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true", help="Parse and show summary without uploading to S3")
    args = parser.parse_args()

    if not args.file.exists():
        sys.exit(f"File not found: {args.file}")

    parse_fn = PARSERS[args.source]
    df = parse_fn(args.file)
    df = add_ingestion_metadata(df, args.source, args.file)

    print(f"Parsed {len(df)} rows from {args.file.name}")
    print(df.head())

    # TODO: add validation here before writing — e.g. assert no null
    # transaction_date, assert amount is numeric, flag duplicate row_hash
    # within this file, sanity-check date range looks like one statement
    # period, etc. Fail loudly rather than writing bad data to bronze.

    write_to_bronze(df, args.source, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
