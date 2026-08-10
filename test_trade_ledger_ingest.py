"""
Smoke test for trade_ledger_ingest.py.

Runs the real parser against a downloaded sample file and asserts the
output looks structurally sane — no S3 calls, no writes. This is a manual
check to run whenever you're validating the parser against a new/changed
export format (e.g. Fidelity tweaks their CSV layout), not a CI test suite.

Directory layout assumed:
    project_root/
      bronze/
        trade_ledger_ingest.py
      personal_data/
        broker_exports/
          fidelity_2025.csv
      test_trade_ledger_ingest.py   <- this file

Usage:
    python test_trade_ledger_ingest.py
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.append(str(PROJECT_ROOT / "bronze"))

from trade_ledger_ingest import parse_trade_ledger, add_ingestion_metadata, add_partition_key  # noqa: E402

TEST_FILE = PROJECT_ROOT / "personal_data" / "broker_exports" / "fidelity_2025.csv"


def run():
    if not TEST_FILE.exists():
        sys.exit(f"Test file not found: {TEST_FILE}\n"
                  f"Update TEST_FILE at the top of this script if your path differs.")

    df = parse_trade_ledger(TEST_FILE)
    df = add_ingestion_metadata(df, TEST_FILE)

    print(f"Parsed {len(df)} rows from {TEST_FILE.name}\n")
    print(df.dtypes, "\n")
    print(df.head(), "\n")

    # --- structural sanity checks ---
    # These belong at ingestion time because a failure here means the file
    # itself is malformed or the parser's assumptions (skiprows/skipfooter/
    # column names) drifted — not something silver should have to catch.
    assert len(df) > 0, "No rows parsed — check HEADER_SKIPROWS / FOOTER_SKIPROWS offsets"
    assert df["trade_date"].isnull().sum() == 0, "Null trade_date values found"
    assert df["symbol"].isnull().sum() == 0, "Null symbol values found"
    dupes = df[df.duplicated(subset="row_hash", keep=False)].sort_values("row_hash")
    if len(dupes):
        print(f"\n{len(dupes)} rows share a row_hash with at least one other row:")
        print(dupes[["trade_date", "account", "action", "symbol", "quantity", "price", "amount"]])

    assert df["row_hash"].nunique() == len(df), (

        "Duplicate row_hash WITHIN this single file — either genuine duplicate "
        "trades, or hash_cols isn't specific enough to distinguish real rows"
    )

    # Sign convention check — buys negative, sells/dividends positive.
    # TODO: adjust the string match below once you see the real 'Action'
    # values in your file (e.g. 'YOU BOUGHT', 'YOU SOLD', 'DIVIDEND RECEIVED')
    buys = df[df["action"].str.contains("bought", case=False, na=False)]
    sells = df[df["action"].str.contains("sold", case=False, na=False)]
    if len(buys):
        assert (buys["amount"] < 0).all(), "Found a buy row with non-negative amount — sign convention broke"
        print(f"Buy sign check passed ({len(buys)} rows)")
    if len(sells):
        assert (sells["amount"] > 0).all(), "Found a sell row with non-positive amount — sign convention broke"
        print(f"Sell sign check passed ({len(sells)} rows)")

    # Partition preview — confirms multi-month splitting works as expected,
    # without actually writing anything to S3 or /tmp.
    df = add_partition_key(df, "trade_date")
    print("\nRows per partition:")
    print(df["year_month"].value_counts().sort_index())

    print("\nAll checks passed.")


if __name__ == "__main__":
    run()
