"""
Exports gold-layer dbt models to Parquet files for Power BI to read directly
(via Get Data > Folder) — no ODBC driver / custom connector needed

Run this AFTER `dbt run`, from the repo root:
    python export_gold_to_powerbi.py

"""

import duckdb
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DUCKDB_PATH = PROJECT_ROOT / "dbt_project" / "local.duckdb"  # TODO(you): confirm this path
EXPORT_DIR = PROJECT_ROOT / "exports"

GOLD_TABLES = [
    "wealth_summary",
    "monthly_spending",
    "position_lots",
    "realized_gains",
    "unrealized_gains",
    "investment_growth",
    "portfolio_roe_summary",
]


def main():
    if not DUCKDB_PATH.exists():
        raise SystemExit(
            f"DuckDB file not found at {DUCKDB_PATH}\n"
            f"Update DUCKDB_PATH in this script to match your profiles.yml 'path:' setting."
        )

    EXPORT_DIR.mkdir(exist_ok=True)

    con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

    for table in GOLD_TABLES:
        out_path = EXPORT_DIR / f"{table}.parquet"
        try:
            con.execute(
                f"COPY (SELECT * FROM main_gold.{table}) TO '{out_path}' (FORMAT PARQUET)"
            )
            row_count = con.execute(f"SELECT COUNT(*) FROM main_gold.{table}").fetchone()[0]
            print(f"Exported {table} -> {out_path} ({row_count} rows)")
        except duckdb.CatalogException:
            print(f"WARNING: main_gold.{table} not found — skipping (did dbt run build it?)")

    con.close()
    print(f"\nDone. Point Power BI's 'Get Data > Folder' at: {EXPORT_DIR}")


if __name__ == "__main__":
    main()
