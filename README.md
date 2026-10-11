# Lakehouse ELT

A small but complete **medallion-architecture** data pipeline built on DuckDB and Parquet:
raw JSON events are ingested to **bronze**, cleaned and merged into **silver**, and aggregated into
**gold** tables, with data-quality gates and a DAG runner coordinating the steps.

The input data is **synthetic** (deterministic, seeded) and deliberately messy. It includes late
status updates, duplicates, malformed JSON, negative quantities and inconsistent casing, so every
stage has real work to do.

```
landing/dt=*/orders.jsonl
        |  ingest_bronze   raw text + lineage, append-only, one Parquet partition per day
        v
bronze/orders/ingest_date=*/data.parquet
        |  build_silver    typed, cleaned, deduped; incremental via watermark
        v                  invalid rows -> silver/orders_rejects.parquet (with reason)
silver/orders.parquet
        |  quality_checks  gate: failure stops everything downstream
        v
        |  build_gold      daily_revenue, top_products, customer_summary, status_funnel
        v
gold/*.parquet  --> reconcile_gold (gold revenue must equal silver revenue)
```

## Run it

```bash
pip install -r requirements-dev.txt
python -m lakehouse generate --days 5 --orders 200 --start 2026-10-01
python -m lakehouse run
python -m lakehouse query "SELECT order_date, orders, revenue FROM daily_revenue"
python -m lakehouse query "SELECT reject_reason, COUNT(*) FROM rejects GROUP BY 1"
python -m pytest
```

`query` exposes views named `bronze`, `silver`, `rejects`, and each gold table. Data lives in `./data`
(override with `--root`).

## Key design decisions

| Decision | Why |
|----------|-----|
| Bronze stores everything as text | A bad cast can never destroy raw data; typing happens in silver |
| One bronze partition per landing day, skipped if present | Ingestion is idempotent; re-running is safe |
| Silver keeps the latest `event_ts` per order | Correct under late and out-of-order events |
| Silver tracks a watermark in `state.json` | Only new partitions are processed (incremental) |
| Rejected rows go to a quarantine table with a reason | Nothing disappears silently; bad data stays debuggable |
| Quality gate between silver and gold | Bad data never reaches consumers |
| Gold reconciliation against silver | Catches aggregation bugs |
| `QualityError` is never retried | Retrying a deterministic failure only wastes time |

## Project layout

```
lakehouse/        config, generate, bronze, silver, gold, quality, runner, pipeline, cli
dags/             reference Airflow DAG (not executed by the tests)
tests/            22 tests: each layer, the DAG runner, and end-to-end runs
```

## Honest limitations

- Single machine. Silver is fully rewritten on each incremental run (fine for this size; at scale you
  would partition silver and merge only affected partitions, or use Delta Lake / Iceberg).
- The watermark is partition-based, so it assumes bronze partitions arrive in date order.
- `dags/lakehouse_dag.py` shows the Airflow mapping but was not run in this repo.
- Data is synthetic. Do not present it as production data.

## Ideas to extend it

1. Replace the JSON state file with a table, or add run metadata (rows in/out per run).
2. Add a `dim_customer` with SCD Type 2 history.
3. Partition silver by `order_date` and merge only changed partitions.
4. Swap the local filesystem for S3 using DuckDB's `httpfs`.
5. Add schema-drift detection for new or renamed columns in landing files.
6. Expose gold tables through the API project's style of service, or a small dashboard.
