"""
Bronze layer ingestion: daily stock prices (yfinance) -> partitioned Parquet on S3

Ticker source (hybrid, per your call):
  1. Auto-derived from DISTINCT symbols already present in bronze/trades
     (queried directly from S3 via DuckDB + httpfs — no separate copy needed)
  2. Supplemented by an optional static watchlist file (watchlist.txt, one
     ticker per line, project root) for tickers you want price history on
     without having traded them yet
Both sources are merged and deduped.

Partitioned by trading date:
    s3://<bucket>/bronze/stock_prices/date=2026-08-10/*.parquet

Usage:
    python ingest_stock_prices.py --dry-run
    python ingest_stock_prices.py --start 2026-08-01 --end 2026-08-10
    python ingest_stock_prices.py --tickers NVDA,COIN --dry-run   # ad-hoc, on top of auto+watchlist
"""

import argparse
import hashlib
import os
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import duckdb
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent.parent
WATCHLIST_FILE = PROJECT_ROOT / "watchlist.txt"

S3_BUCKET = os.environ["WEALTH_LAKEHOUSE_BUCKET"]
BRONZE_PREFIX = "bronze/stock_prices"
TRADES_PREFIX = "bronze/trades"
AWS_REGION = os.environ["AWS_REGION"]


# ---------------------------------------------------------------------------
# Ticker sourcing
# ---------------------------------------------------------------------------

def get_portfolio_symbols() -> set[str]:
    """
    Query DISTINCT symbols directly from bronze/trades on S3 via DuckDB's
    httpfs extension — no local copy of trade data needed for this.
    Returns an empty set (with a warning, not a crash) if bronze/trades
    doesn't exist yet or is unreachable, so this script still works standalone.
    """
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_region='{AWS_REGION}';")

    if "AWS_ACCESS_KEY_ID" in os.environ and "AWS_SECRET_ACCESS_KEY" in os.environ:
        con.execute(f"SET s3_access_key_id='{os.environ['AWS_ACCESS_KEY_ID']}';")
        con.execute(f"SET s3_secret_access_key='{os.environ['AWS_SECRET_ACCESS_KEY']}';")

    s3_glob = f"s3://{S3_BUCKET}/{TRADES_PREFIX}/**/*.parquet"
    try:
        result = con.execute(f"SELECT DISTINCT symbol FROM read_parquet('{s3_glob}')").fetchall()
        symbols = {row[0] for row in result if row[0]}
        print(f"Found {len(symbols)} distinct symbols in bronze/trades")
        return symbols
    except duckdb.IOException:
        print("WARNING: could not read bronze/trades (no files yet?) — continuing with watchlist only")
        return set()


def get_watchlist_symbols() -> set[str]:
    """Optional static ticker list, one per line, blank lines and # comments ignored."""
    if not WATCHLIST_FILE.exists():
        return set()
    lines = WATCHLIST_FILE.read_text().splitlines()
    symbols = {line.strip().upper() for line in lines if line.strip() and not line.strip().startswith("#")}
    print(f"Found {len(symbols)} symbols in watchlist.txt")
    return symbols


# ---------------------------------------------------------------------------
# Price fetch
# ---------------------------------------------------------------------------

def fetch_prices(tickers: list[str], start: str, end: str) -> pd.DataFrame:
    """
    Pulls daily OHLCV for all tickers in one batched yfinance call.
    TODO: yfinance's `end` is exclusive — pass end date + 1 day if you want
    that calendar day included. Handled in main() below.
    """
    if not tickers:
        sys.exit("No tickers to fetch — bronze/trades is empty and watchlist.txt has no entries.")

    print(f"Fetching {len(tickers)} tickers from {start} to {end}: {sorted(tickers)}")
    raw = yf.download(tickers, start=start, end=end, group_by="ticker", auto_adjust=False, progress=False)

    rows = []
    for ticker in tickers:
        try:
            df_t = raw[ticker].reset_index() if len(tickers) > 1 else raw.reset_index()
        except KeyError:
            print(f"WARNING: no data returned for {ticker} — delisted, wrong symbol, or no trading days in range")
            continue
        df_t["symbol"] = ticker
        rows.append(df_t)

    if not rows:
        sys.exit("No price data returned for any ticker — check date range and ticker symbols.")

    df = pd.concat(rows, ignore_index=True)
    df = df.rename(columns={
        "Date": "price_date",
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Adj Close": "adj_close",
        "Volume": "volume",
    })
    df["price_date"] = pd.to_datetime(df["price_date"]).dt.date
    # Default currency for lib
    df["currency"] = "USD"

    cols = ["price_date", "symbol", "open", "high", "low", "close", "adj_close", "volume", "currency"]
    return df[cols].dropna(subset=["close"])  # drop non-trading-day gaps yfinance sometimes includes


# ---------------------------------------------------------------------------
# Common ingestion logic (same pattern as bank/trade scripts)
# ---------------------------------------------------------------------------

def add_ingestion_metadata(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["source"] = "stock_prices"
    df["ingested_at"] = datetime.now(timezone.utc)

    hash_cols = ["price_date", "symbol", "close"]
    df["row_hash"] = df[hash_cols].apply(
        lambda row: hashlib.sha256("|".join(str(v) for v in row).encode()).hexdigest(),
        axis=1,
    )
    return df


def write_to_bronze(df: pd.DataFrame, dry_run: bool = False) -> None:
    """Partition by trading date and write one Parquet file per date to S3."""
    s3 = boto3.client("s3", region_name=AWS_REGION)

    for price_date, group in df.groupby("price_date"):
        local_tmp = Path(tempfile.gettempdir()) / f"stock_prices_{price_date}.parquet"
        group.to_parquet(local_tmp, index=False)

        s3_key = f"{BRONZE_PREFIX}/date={price_date}/{local_tmp.name}"

        if dry_run:
            print(f"[dry-run] would upload {local_tmp} -> s3://{S3_BUCKET}/{s3_key} ({len(group)} rows)")
            local_tmp.unlink(missing_ok=True)
            continue

        s3.upload_file(str(local_tmp), S3_BUCKET, s3_key)
        print(f"Uploaded {len(group)} rows -> s3://{S3_BUCKET}/{s3_key}")
        local_tmp.unlink()


def main():
    parser = argparse.ArgumentParser(description="Ingest daily stock prices into the bronze layer.")
    parser.add_argument("--start", type=str, default=None, help="YYYY-MM-DD, default: 5 days ago (covers weekends/holidays)")
    parser.add_argument("--end", type=str, default=None, help="YYYY-MM-DD, default: today")
    parser.add_argument("--tickers", type=str, default=None, help="Comma-separated extra tickers, added on top of auto+watchlist")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    end_date = datetime.strptime(args.end, "%Y-%m-%d").date() if args.end else datetime.now().date()
    start_date = datetime.strptime(args.start, "%Y-%m-%d").date() if args.start else end_date - timedelta(days=5)

    symbols = get_portfolio_symbols() | get_watchlist_symbols()
    if args.tickers:
        symbols |= {t.strip().upper() for t in args.tickers.split(",") if t.strip()}

    df = fetch_prices(sorted(symbols), start=str(start_date), end=str(end_date + timedelta(days=1)))  # +1: yfinance end is exclusive
    df = add_ingestion_metadata(df)

    print(f"\nParsed {len(df)} price rows across {df['symbol'].nunique()} symbols")
    print(df.head())

    # TODO: add validation before writing — e.g. assert no null close,
    # assert price_date range matches what was requested, flag any ticker
    # in `symbols` that returned zero rows (already warned above, but not
    # currently a hard failure).

    write_to_bronze(df, dry_run=args.dry_run)


if __name__ == "__main__":
    main()