"""Bronze layer: raw landing files -> append-only Parquet, one partition per landing day.

Bronze keeps every value as text, exactly as received, plus lineage columns. Typing and
cleaning happen in silver so raw data is never lost to a bad cast.
"""

from __future__ import annotations

import json

import duckdb

from .config import Paths, quote_path

COLUMNS = ["order_id", "customer_id", "product_id", "quantity", "unit_price", "status", "event_ts"]


def ingest_bronze(con: duckdb.DuckDBPyConnection, paths: Paths, force: bool = False) -> dict:
    """Ingest each landing partition once. Re-running is a no-op unless force=True."""
    ingested: list[str] = []
    skipped: list[str] = []
    malformed = 0
    rows_total = 0

    for part in sorted(paths.landing.glob("dt=*")):
        dt = part.name.split("=", 1)[1]
        out_dir = paths.bronze / f"ingest_date={dt}"
        out = out_dir / "data.parquet"
        if out.exists() and not force:
            skipped.append(dt)
            continue

        rows: list[tuple] = []
        for file in sorted(part.glob("*.jsonl")):
            for line_no, line in enumerate(file.read_text(encoding="utf-8").splitlines(), start=1):
                if not line.strip():
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue
                if not isinstance(obj, dict):
                    malformed += 1
                    continue
                values = tuple(None if obj.get(c) is None else str(obj[c]) for c in COLUMNS)
                rows.append(values + (file.name, line_no))

        out_dir.mkdir(parents=True, exist_ok=True)
        con.execute(
            """CREATE OR REPLACE TEMP TABLE _stage (
                   order_id VARCHAR, customer_id VARCHAR, product_id VARCHAR,
                   quantity VARCHAR, unit_price VARCHAR, status VARCHAR, event_ts VARCHAR,
                   _source_file VARCHAR, _line_no INTEGER)"""
        )
        if rows:
            con.executemany("INSERT INTO _stage VALUES (?,?,?,?,?,?,?,?,?)", rows)
        con.execute(
            f"COPY (SELECT *, current_timestamp AS _ingested_at FROM _stage) "
            f"TO '{quote_path(out)}' (FORMAT PARQUET)"
        )
        rows_total += len(rows)
        ingested.append(dt)

    return {"ingested": ingested, "skipped": skipped, "malformed_lines": malformed, "rows": rows_total}
