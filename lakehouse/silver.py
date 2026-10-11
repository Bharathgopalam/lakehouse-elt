"""Silver layer: typed, cleaned, deduplicated, one current row per order.

Incremental: only bronze partitions newer than the stored watermark are processed, then
merged with the existing silver table. Within an order, the version with the latest
event_ts wins, so late or out-of-order events cannot overwrite newer state.
Rows failing validation go to a quarantine table with a reason, never silently dropped.
"""

from __future__ import annotations

import duckdb

from .config import VALID_STATUSES, Paths, quote_path
from .state import load_state, save_state

_STATUS_LIST = ", ".join(f"'{s}'" for s in VALID_STATUSES)


def build_silver(con: duckdb.DuckDBPyConnection, paths: Paths) -> dict:
    state = load_state(paths.state_file)
    watermark = state.get("silver_watermark")

    all_parts = sorted(p.name.split("=", 1)[1] for p in paths.bronze.glob("ingest_date=*"))
    new_parts = [d for d in all_parts if watermark is None or d > watermark]
    if not new_parts:
        return {"new_partitions": [], "rows_in": 0, "valid": 0, "rejected": 0,
                "silver_rows": _count(con, paths.silver)}

    file_list = ", ".join(
        f"'{quote_path(paths.bronze / f'ingest_date={d}' / 'data.parquet')}'" for d in new_parts
    )
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE _bronze_new AS
            SELECT *, CAST(regexp_extract(filename, 'ingest_date=([0-9-]+)', 1) AS DATE) AS ingest_date
            FROM read_parquet([{file_list}], filename = true)"""
    )

    con.execute(
        """CREATE OR REPLACE TEMP TABLE _typed AS
           SELECT
               NULLIF(trim(order_id), '')   AS order_id,
               NULLIF(trim(customer_id), '') AS customer_id,
               NULLIF(trim(product_id), '') AS product_id,
               TRY_CAST(quantity AS INTEGER)       AS quantity,
               TRY_CAST(unit_price AS DECIMAL(10,2)) AS unit_price,
               lower(trim(status))                 AS status,
               TRY_CAST(event_ts AS TIMESTAMP)     AS event_ts,
               ingest_date, _source_file, _line_no
           FROM _bronze_new"""
    )
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE _classified AS
            SELECT *, CASE
                WHEN order_id IS NULL THEN 'missing order_id'
                WHEN customer_id IS NULL THEN 'missing customer_id'
                WHEN product_id IS NULL THEN 'missing product_id'
                WHEN quantity IS NULL OR quantity <= 0 THEN 'invalid quantity'
                WHEN unit_price IS NULL OR unit_price < 0 THEN 'invalid unit_price'
                WHEN status IS NULL OR status NOT IN ({_STATUS_LIST}) THEN 'invalid status'
                WHEN event_ts IS NULL THEN 'invalid event_ts'
            END AS reject_reason
            FROM _typed"""
    )

    rows_in = _scalar(con, "SELECT COUNT(*) FROM _classified")
    rejected = _scalar(con, "SELECT COUNT(*) FROM _classified WHERE reject_reason IS NOT NULL")

    # Merge new valid rows with existing silver, then keep the latest version per order.
    existing_sql = (
        f"UNION ALL SELECT order_id, customer_id, product_id, quantity, unit_price, status, "
        f"event_ts, ingest_date, order_date FROM read_parquet('{quote_path(paths.silver)}')"
        if paths.silver.exists() else ""
    )
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE _versions AS
            SELECT order_id, customer_id, product_id, quantity, unit_price, status, event_ts,
                   ingest_date, CAST(event_ts AS DATE) AS order_date
            FROM _classified WHERE reject_reason IS NULL
            {existing_sql}"""
    )
    con.execute(
        """CREATE OR REPLACE TEMP TABLE _merged AS
           SELECT order_id, customer_id, product_id, quantity, unit_price, status, event_ts,
                  ingest_date, first_order_date AS order_date,
                  CAST(quantity * unit_price AS DECIMAL(12,2)) AS order_total
           FROM (
               SELECT *,
                      ROW_NUMBER() OVER (PARTITION BY order_id
                                         ORDER BY event_ts DESC, ingest_date DESC) AS rn,
                      MIN(order_date) OVER (PARTITION BY order_id) AS first_order_date
               FROM _versions)
           WHERE rn = 1"""
    )

    paths.silver.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY (SELECT * FROM _merged ORDER BY order_id) TO '{quote_path(paths.silver)}' (FORMAT PARQUET)")

    reject_cols = ("order_id, customer_id, product_id, quantity, unit_price, status, event_ts, "
                   "ingest_date, _source_file, _line_no, reject_reason")
    prior = (f"UNION ALL SELECT {reject_cols} FROM read_parquet('{quote_path(paths.quarantine)}')"
             if paths.quarantine.exists() else "")
    con.execute(
        f"""CREATE OR REPLACE TEMP TABLE _quarantine AS
            SELECT {reject_cols} FROM _classified WHERE reject_reason IS NOT NULL
            {prior}"""
    )
    con.execute(f"COPY _quarantine TO '{quote_path(paths.quarantine)}' (FORMAT PARQUET)")

    state["silver_watermark"] = max(new_parts)
    save_state(paths.state_file, state)

    return {
        "new_partitions": new_parts,
        "rows_in": rows_in,
        "valid": rows_in - rejected,
        "rejected": rejected,
        "silver_rows": _count(con, paths.silver),
    }


def _scalar(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def _count(con: duckdb.DuckDBPyConnection, path) -> int:
    if not path.exists():
        return 0
    return _scalar(con, f"SELECT COUNT(*) FROM read_parquet('{quote_path(path)}')")
