# lakehouse-elt

A learning project where I practice the Bronze / Silver / Gold (medallion) pattern on a small scale.
At work I used Azure Data Factory and Snowflake, so I wanted to see the same ideas without a cloud bill:
everything here runs locally with Python, DuckDB and Parquet files.

The input is fake order data that I generate with a script. It is messy on purpose: duplicate events,
late status updates, broken JSON lines, negative quantities, and mixed upper/lower case, so each layer
has something to fix.

## How it flows

```
landing (json files per day)
   -> bronze   raw text + where it came from, one Parquet folder per day
   -> silver   typed, cleaned, one row per order, bad rows go to a rejects table
   -> quality checks (if these fail, the run stops)
   -> gold     daily revenue, top products, customer summary, status funnel
```

## Running it

```bash
pip install -r requirements-dev.txt
python -m lakehouse generate --days 5 --orders 200 --start 2026-10-01
python -m lakehouse run
python -m lakehouse query "SELECT order_date, orders, revenue FROM daily_revenue"
python -m pytest
```

## Things I focused on

- Bronze keeps everything as text, so a bad value can't break ingestion. Types are applied in silver.
- Re-running is safe: a day that's already in bronze is skipped.
- Silver keeps the newest event for each order, so a late update can't overwrite a newer status.
- Silver only processes new days (it stores the last processed day in a small state file).
- Bad rows are not dropped silently. They go to a rejects table with the reason.
- Gold totals are checked against silver so I know the aggregations add up.

## What's missing / what I'd change

- It runs on one machine and rewrites silver every time. On real data volumes I'd partition silver and
  only merge the changed days, or use Delta Lake / Iceberg.
- The "last processed day" logic assumes days arrive in order.
- There's an Airflow DAG file in `dags/` to show how the steps would map to Airflow, but I haven't run it
  in Airflow yet.
- The data is synthetic.

## Next things I want to try

- Add a customer dimension with SCD Type 2 history
- Read and write from S3 instead of local files
- Detect new or renamed columns in the landing files
