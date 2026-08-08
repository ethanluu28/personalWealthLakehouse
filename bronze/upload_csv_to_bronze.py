"""
Small helper for the manual side of ingestion: bank export CSVs and your
trade ledger aren't pulled from an API, so this just pushes a local file
into the right bronze/ prefix with a date partition.

Usage:
    python bronze/upload_csv_to_bronze.py expenses ~/Downloads/chase_export.csv
    python bronze/upload_csv_to_bronze.py trades ~/Documents/trades.csv
"""
import os
import sys
from datetime import date

import boto3
from dotenv import load_dotenv

load_dotenv()

BUCKET = os.environ["WEALTH_LAKEHOUSE_BUCKET"]
VALID_KINDS = {"expenses", "trades"}


def upload(kind: str, local_path: str) -> str:
    if kind not in VALID_KINDS:
        raise ValueError(f"kind must be one of {VALID_KINDS}, got {kind!r}")

    today = date.today().isoformat()
    filename = os.path.basename(local_path)
    key = f"bronze/{kind}/dt={today}/{filename}"

    s3 = boto3.client("s3")
    s3.upload_file(local_path, BUCKET, key)
    return f"s3://{BUCKET}/{key}"


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)

    dest = upload(sys.argv[1], sys.argv[2])
    print(f"Uploaded to {dest}")
