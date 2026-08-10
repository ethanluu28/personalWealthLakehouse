# Personal Wealth Lakehouse

A dbt + DuckDB + Apache Iceberg medallion pipeline for tracking personal
expenses and stock portfolio. Bronze → Silver → Gold, orchestrated daily,
queried with a local BI tool.


## Repo layout

```
bronze/                       ingestion scripts (bronze layer)
dbt_project/                  dbt project (silver + gold layers)
  models/staging/             silver: cleaned Iceberg tables
  models/marts/                gold: wealth_summary mart
personal_data/
  bank_exports/
  broker_exports/
.github/workflows/            daily orchestration (GitHub Actions default)
requirements.txt
.env.example
```

## Step 1 — S3 bucket + IAM

1. Create the bucket, e.g. `your-wealth-lakehouse`. Confirm **Block Public
   Access** is on — this holds real financial data.
2. Leave default encryption as SSE-S3 (free, on by default). Don't bother
   with SSE-KMS for a solo project — it costs ~$1/mo/key for no real benefit
   here.
3. Create a scoped IAM user/role for the pipeline — **not root credentials**.
   Minimum permissions: `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on
   the bucket, plus `glue:*` on the relevant database/tables if you go with
   Glue in Step 3.
4. Set a billing alarm (~$5) as a safety net. Actual data volume here (tens
   of MB) should cost cents/month, but free-tier terms shifted in mid-2025
   depending on when your AWS account was created, so don't rely on it
   being free.
5. Copy `.env.example` to `.env` and fill in the bucket name + credentials.

## Step 2 — Local environment

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Test the stock ingestion script works end to end:

```bash
python bronze/ingest_stock_prices.py
```

Check `s3://<your-bucket>/bronze/stock_prices/dt=<today>/prices.parquet`
landed. Edit the `TICKERS` list in that file to your real holdings first.

For bank exports and your trade ledger (manual, not API-driven), use the
upload helper whenever you have a new CSV:

```bash
python bronze/upload_csv_to_bronze.py expenses ~/Downloads/chase_export.csv
python bronze/upload_csv_to_bronze.py trades ~/Documents/trades.csv
```

## Step 3 — Pick the Iceberg catalog (open decision)

DuckDB can't safely write Iceberg tables straight to S3 — it needs a
catalog to track which metadata file is current per table. Two options,
both sketched in `dbt_project/profiles.yml.example`:


Action: copy `dbt_project/profiles.yml.example` to `~/.dbt/profiles.yml`,
keep Option A uncommented, and run `dbt debug` from `dbt_project/`. If
writes fail repeatedly, comment out Option A and uncomment Option B instead
— don't sink hours into Glue's auth errors on a personal project.

## Step 4 — Pick the orchestrator (open decision)

- **GitHub Actions** (scaffold default, see `.github/workflows/daily_pipeline.yml`)
  — no idle infrastructure, same pattern as your 24hr-booker project. Add
  `WEALTH_LAKEHOUSE_BUCKET`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
  and `AWS_REGION` as repo secrets and it runs daily at 13:00 UTC.


## Step 5 — Run the dbt models

```bash
cd dbt_project
dbt debug      # confirms catalog connection from Step 3
dbt run        # builds stg_stock_prices, stg_transactions, stg_trades, wealth_summary
dbt test       # runs the not_null/unique/accepted_values tests already defined
dbt docs generate && dbt docs serve   # optional: browsable model docs
```

Fix column names in the three staging models first — they currently assume
generic column names (`transaction_date`, `amount`, `category`, etc.) that
won't match your bank's actual CSV headers until you edit them.

## Step 6 — Point a BI tool at gold

`wealth_summary` (one row per day: cash balance, portfolio value, net
worth, day-over-day ROI %) is the table to query. Any of these work,
pick one:

- **Evidence** — markdown+SQL reports, good if you want something
  git-versioned and simple
- **Streamlit** — more control, good if you want interactive filters
- **Power BI Desktop** — good if you already know it

All three can point DuckDB at the S3 Iceberg gold table directly.

## Still open / not yet built

- [ ] Incremental vs. full-refresh strategy for daily stock price ingestion
      (scaffold currently does full-refresh via `read_parquet` glob —
      fine at this data volume, but worth revisiting if it grows)
- [ ] `dbt docs generate` wired into CI (currently manual, Step 5)
- [ ] Final BI tool choice (Step 6)
