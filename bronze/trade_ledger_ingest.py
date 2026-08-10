"""
Bronze layer ingestion: trade ledger CSV -> partitioned Parquet on S3

Design:
- Mirrors the pattern in bronze_ingest_bank_csv.py: parse -> stamp lineage
  metadata + row_hash -> partition by year_month -> write Parquet to S3.
- Source is a Fidelity-style brokerage export (confirmed columns: Run Date,
  Account, Account Number, Action, Symbol, Description, Type, Exchange,
  Exchange, Currency, Price, Quantity, Exchange, Commission, Fees,
  Accrued Interest, Amount, Settlement Date). Only a subset is kept — see
  KEEP_COLUMNS below.
- The real header row is NOT the first row — there are 2 metadata rows
  above it in the export (same pattern as the Amex file), hence skiprows=2.
  TODO: reconfirm this offset if Fidelity changes their export format.
- Amount sign convention (confirmed): buys are NEGATIVE, sells and
  dividends are POSITIVE. Kept as-is from the source file — no flip needed,
  unlike the Chase bank CSV case.
- Partitioned by trade month:
    s3://<bucket>/bronze/trades/year_month=2026-08/*.parquet

Usage:
    python trade_ledger_ingest.py --file ~/Downloads/fidelity_trades_2026.csv
    python trade_ledger_ingest.py --file ~/Downloads/fidelity_trades_2026.csv --dry-run
"""

import argparse
import hashlib
import os
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------------------
# Config — loaded from env vars so credentials/identifiers never live in the
# repo itself. Same .env as the bank ingestion script.
# ---------------------------------------------------------------------------
S3_BUCKET = os.environ["WEALTH_LAKEHOUSE_BUCKET"]
BRONZE_PREFIX = "bronze/trades"
AWS_REGION = os.environ["AWS_REGION"]

HEADER_SKIPROWS = 2
FOOTER_SKIPROWS = 11

KEEP_COLUMNS = ["Run Date", "Account", "Action", "Symbol", "Currency", "Price", "Quantity", "Amount"]


def account_label_from_filename(file_path: Path) -> str:
    """Same convention as the bank ingestion script — no personal/account
    data pulled from file contents, just the filename"""
    return file_path.stem.split("_")[0]


def parse_trade_ledger(csv_path: Path) -> pd.DataFrame:
    """
    Parses the Fidelity-style trade ledger export into a normalized schema:
      trade_date (date), action (str), symbol (str), currency (str),
      price (float), quantity (float), amount (float), source_account_hint (str)
    """
    df = pd.read_csv(csv_path, skiprows=HEADER_SKIPROWS, skipfooter=FOOTER_SKIPROWS, engine="python")

    # TODO: verify these exact column names against your real downloaded
    # file — Fidelity sometimes has duplicate header names (e.g. 'Exchange'
    # appears 3 times in your snippet), which can shift how pandas reads
    # them. If this KeyErrors, print df.columns.tolist() first like we did
    # for the Amex file.
    missing = [c for c in KEEP_COLUMNS if c not in df.columns]
    if missing:
        sys.exit(f"Missing expected columns in {csv_path.name}: {missing}\n"
                  f"Actual columns found: {df.columns.tolist()}")

    df = df[KEEP_COLUMNS].copy()

    df = df.rename(columns={
        "Run Date": "trade_date",
        "Account": "account",
        "Action": "action",
        "Symbol": "symbol",
        "Currency": "currency",
        "Price": "price",
        "Quantity": "quantity",
        "Amount": "amount",
    })

    df["account"] = df["account"].astype(str).str.strip()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["action"] = df["action"].astype(str).str.strip()

    # TODO: Price/Quantity/Amount may arrive with $ signs, commas, or
    # parentheses-for-negative depending on export settings — inspect a real
    # file before trusting a plain astype(float). If it fails, strip
    # non-numeric characters first, e.g.:
    #   df[col] = df[col].replace(r'[\$,]', '', regex=True).astype(float)
    df["price"] = df["price"].astype(float)
    df["quantity"] = df["quantity"].astype(float)
    df["amount"] = df["amount"].astype(float)  # buys negative, sells/dividends positive — confirmed, no flip

    df["source_account_hint"] = account_label_from_filename(csv_path)

    return df


def add_ingestion_metadata(df: pd.DataFrame, source_file: Path) -> pd.DataFrame:
    """Stamp every row with lineage metadata and a row_hash to dedup downstream"""
    df = df.copy()
    df["source"] = "trade_ledger"
    df["source_file"] = source_file.name
    df["ingested_at"] = datetime.now(timezone.utc)

    hash_cols = ["trade_date", "account", "action", "symbol", "quantity", "price", "amount"]
    base_key = df[hash_cols].apply(lambda row: "|".join(str(v) for v in row), axis=1)
    occurrence = base_key.groupby(base_key).cumcount().astype(str)

    df["row_hash"] = (base_key + "|" + occurrence).apply(
        lambda s: hashlib.sha256(s.encode()).hexdigest()
    )
    return df


def add_partition_key(df: pd.DataFrame, date_column: str) -> pd.DataFrame:
    """Adds a 'year_month' column so a multi-month export splits correctly
    across partitions before writing (same helper pattern as the bank script)."""
    df = df.copy()
    dt = pd.to_datetime(df[date_column])
    df["year_month"] = dt.dt.to_period("M").astype(str)
    return df


def write_to_bronze(df: pd.DataFrame, dry_run: bool = False) -> None:
    """Split by year_month and write one Parquet file per partition to S3."""
    df = add_partition_key(df, "trade_date")

    s3 = boto3.client("s3", region_name=AWS_REGION)

    for year_month, group in df.groupby("year_month"):
        local_tmp = Path(tempfile.gettempdir()) / f"trades_{year_month}.parquet"
        group.drop(columns=["year_month"]).to_parquet(local_tmp, index=False)

        s3_key = f"{BRONZE_PREFIX}/year_month={year_month}/{local_tmp.name}"

        if dry_run:
            print(f"[dry-run] would upload {local_tmp} -> s3://{S3_BUCKET}/{s3_key}")
            local_tmp.unlink(missing_ok=True)
            continue

        s3.upload_file(str(local_tmp), S3_BUCKET, s3_key)
        print(f"Uploaded {len(group)} rows -> s3://{S3_BUCKET}/{s3_key}")

        local_tmp.unlink()


def main():
    parser = argparse.ArgumentParser(description="Ingest a Fidelity-style trade ledger CSV into the bronze layer.")
    parser.add_argument("--file", required=True, type=Path, help="Path to the downloaded trade ledger CSV")
    parser.add_argument("--dry-run", action="store_true", help="Parse and show summary without uploading to S3")
    args = parser.parse_args()

    args.file = args.file.expanduser()
    if not args.file.exists():
        sys.exit(f"File not found: {args.file}")

    df = parse_trade_ledger(args.file)
    df = add_ingestion_metadata(df, args.file)

    print(f"Parsed {len(df)} rows from {args.file.name}")
    print(df.head())

    # TODO: add validation before writing — e.g. assert no null trade_date,
    # assert quantity/price/amount are numeric and non-null, sanity-check
    # that buy rows have negative amount and sell/dividend rows have
    # positive amount (catches a sign-convention surprise early), flag
    # duplicate row_hash within this file.

    write_to_bronze(df, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
