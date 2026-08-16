"""
Bronze layer ingestion: trade ledger CSV -> partitioned Parquet on S3

- Mirrors the pattern in bronze_ingest_bank_csv.py: parse -> stamp lineage
  metadata + row_hash -> partition by year_month -> write Parquet to S3.

    s3://<bucket>/bronze/trades/source=fidelity/year_month=2026-08/*.parquet

Usage:
    python trade_ledger_ingest.py --source fidelity --file ~/Downloads/vanguard_trades_2026.csv
    python trade_ledger_ingest.py --source vanguard --file ~/Downloads/vanguard_2026.csv --dry-run
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

FIDELITY_HEADER_SKIPROWS = 2
FIDELITY_FOOTER_SKIPROWS = 11
FIDELITY_KEEP_COLUMNS = ["Run Date", "Account", "Action", "Symbol", "Currency", "Price", "Quantity", "Amount"]

VANGUARD_HEADER_SKIPROWS = 21
VANGUARD_FOOTER_SKIPROWS = 0
VANGUARD_KEEP_COLUMNS = ["Trade Date", "Account Number", "Transaction Description", "Symbol", "Shares", "Share Price", "Net Amount"]


def account_label_from_filename(file_path: Path) -> str:
    """Same convention as the bank ingestion script — no personal/account
    data pulled from file contents, just the filename"""
    return file_path.stem.split("_")[0]


def parse_fidelity(csv_path: Path) -> pd.DataFrame:
    """
    Parses the Fidelity-style trade ledger export into a normalized schema:
      trade_date (date), account (str), action (str), symbol (str),
      currency (str), price (float), quantity (float), amount (float),
      source_account_hint (str)

    """
    df = pd.read_csv(csv_path, skiprows=FIDELITY_HEADER_SKIPROWS, skipfooter=FIDELITY_FOOTER_SKIPROWS, engine="python")

    missing = [c for c in FIDELITY_KEEP_COLUMNS if c not in df.columns]
    if missing:
        sys.exit(f"Missing expected columns in {csv_path.name}: {missing}\n"
                  f"Actual columns found: {df.columns.tolist()}")

    df = df[FIDELITY_KEEP_COLUMNS].copy()

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
    df["price"] = df["price"].astype(float)
    df["quantity"] = df["quantity"].astype(float)
    df["amount"] = df["amount"].astype(float)  # buys negative, sells/dividends positive — confirmed, no flip

    df["source_account_hint"] = account_label_from_filename(csv_path)

    return df


def parse_vanguard(csv_path: Path) -> pd.DataFrame:
    """
    Parses a Vanguard trade ledger export into the SAME common schema as
    parse_fidelity: trade_date (date), account (str), action (str),
    symbol (str), currency (str), price (float), quantity (float),
    amount (float), source_account_hint (str).

    """
    df = pd.read_csv(csv_path, skiprows=VANGUARD_HEADER_SKIPROWS, skipfooter=VANGUARD_FOOTER_SKIPROWS, engine="python")

    missing = [c for c in VANGUARD_KEEP_COLUMNS if c not in df.columns]
    if missing:
        sys.exit(f"Missing expected columns in {csv_path.name}: {missing}\n"
                  f"Actual columns found: {df.columns.tolist()}")

    df = df[VANGUARD_KEEP_COLUMNS].copy()

    df = df.rename(columns={
        "Trade Date": "trade_date",
        "Account Number": "account",
        "Transaction Description": "action",
        "Symbol": "symbol",
        "Shares": "quantity",
        "Share Price": "price",
        "Net Amount": "amount",
    })

    df["account"] = df["account"].astype(str).str.strip()
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce").dt.date
    df["symbol"] = df["symbol"].astype(str).str.strip()
    df["action"] = df["action"].astype(str).str.strip()
    df["price"] = df["price"].astype(float)
    df["quantity"] = df["quantity"].astype(float)
    df["amount"] = df["amount"].astype(float)

    df["currency"] = "USD"
    df["source_account_hint"] = account_label_from_filename(csv_path)

    return df


def add_ingestion_metadata(df: pd.DataFrame, source: str, source_file: Path) -> pd.DataFrame:
    """Stamp every row with lineage metadata and a row_hash to dedup downstream.
    `source` is the broker identifier (e.g. 'fidelity') — matches the same
    meaning as `source` in bronze_ingest_bank_csv.py (issuer/institution),
    not a fixed 'trade_ledger' tag, so multiple brokers stay distinguishable."""
    df = df.copy()
    df["source"] = source
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


def write_to_bronze(df: pd.DataFrame, source: str, dry_run: bool = False) -> None:
    """Split by year_month and write one Parquet file per partition to S3,
    partitioned by broker (source=) same as bronze_ingest_bank_csv.py — this
    is what prevents a second broker's files from colliding with Fidelity's
    in the same month."""
    df = add_partition_key(df, "trade_date")

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


PARSERS = {
    "fidelity": parse_fidelity,
    "vanguard": parse_vanguard,
}


def main():
    parser = argparse.ArgumentParser(description="Ingest a broker trade ledger CSV into the bronze layer.")
    parser.add_argument("--source", required=True, choices=PARSERS.keys(), help="Which broker export this is")
    parser.add_argument("--file", required=True, type=Path, help="Path to the downloaded trade ledger CSV")
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

    # --- Validation before writing ---

    # Check 1: null trade_date — real parsing failure, hard fail.
    null_dates = df["trade_date"].isnull().sum()
    assert null_dates == 0, f"Validation Failed: Found {null_dates} rows with null trade_date."

    # Check 2: amount numeric/non-null — required for every row type (trades, dividends, interest alike), hard fail.
    null_amounts = pd.to_numeric(df["amount"], errors="coerce").isnull().sum()
    assert null_amounts == 0, f"Validation Failed: Found {null_amounts} rows where 'amount' is not numeric."

    # Check 3: price/quantity null on BUY/SELL/REINVESTMENT rows specifically soft warning
    trade_rows = df[df["action"].str.contains("bought|sold|reinvestment", case=False, na=False)]
    bad_trade_rows = trade_rows[trade_rows["price"].isnull() | trade_rows["quantity"].isnull()]
    if len(bad_trade_rows):
        print(f"Warning: {len(bad_trade_rows)} buy/sell/reinvestment rows have a null price or quantity:")
        print(bad_trade_rows[["trade_date", "account", "action", "symbol", "price", "quantity", "amount"]])

    # Check 4: sign convention (buys/reinvestments negative, sells/dividends positive) — soft warning
    buys = df[df["action"].str.contains("bought|reinvestment", case=False, na=False)]
    sells_or_divs = df[df["action"].str.contains("sold|dividend", case=False, na=False)]
    bad_buys = buys[buys["amount"] >= 0]
    bad_sells = sells_or_divs[sells_or_divs["amount"] <= 0]
    if len(bad_buys):
        print(f"Warning: {len(bad_buys)} buy rows have non-negative amount — sign convention may have broken:")
        print(bad_buys[["trade_date", "account", "action", "symbol", "amount"]])
    if len(bad_sells):
        print(f"Warning: {len(bad_sells)} sell/dividend rows have non-positive amount — sign convention may have broken:")
        print(bad_sells[["trade_date", "account", "action", "symbol", "amount"]])

    # Check 5: duplicate row_hash — soft warning
    duplicate_count = df["row_hash"].duplicated().sum()
    if duplicate_count > 0:
        print(f"Warning: Found {duplicate_count} duplicate row_hash entries in this file — investigate, but continuing.")

    write_to_bronze(df, args.source, dry_run=args.dry_run)


if __name__ == "__main__":
    main()