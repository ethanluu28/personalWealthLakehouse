"""
Pulls daily close prices for a fixed ticker list and lands them as a raw
Parquet file in the bronze layer of the lakehouse bucket.

Run manually:      python bronze/ingest_stock_prices.py
Run in CI:          see .github/workflows/daily_pipeline.yml

TODO(you): replace TICKERS with your actual portfolio holdings, pulled
from your trade ledger once that's wired up, instead of hardcoding.
"""
import os
from datetime import date

import boto3
import pandas as pd
import yfinance as yf
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.environ["WEALTH_LAKEHOUSE_BUCKET"]
TICKERS = ["VTI", "VXUS", "BND"]  # TODO(you): fill in your real holdings


def fetch_prices(tickers: list[str], as_of: date) -> pd.DataFrame:
    """Pull the latest daily close for each ticker."""
    data = yf.download(tickers, period="1d", interval="1d", progress=False)
    closes = data["Close"].iloc[-1]
    return pd.DataFrame(
        {
            "ticker": closes.index,
            "close_price": closes.values,
            "price_date": as_of.isoformat(),
        }
    )


def write_bronze(df: pd.DataFrame, as_of: date) -> str:
    """Write to bronze/stock_prices/, partitioned by date."""
    key = f"bronze/stock_prices/dt={as_of.isoformat()}/prices.parquet"
    local_path = f"/tmp/prices_{as_of.isoformat()}.parquet"
    df.to_parquet(local_path, index=False)

    s3 = boto3.client("s3")
    s3.upload_file(local_path, BUCKET, key)
    return f"s3://{BUCKET}/{key}"


if __name__ == "__main__":
    today = date.today()
    prices = fetch_prices(TICKERS, today)
    dest = write_bronze(prices, today)
    print(f"Wrote {len(prices)} rows to {dest}")
