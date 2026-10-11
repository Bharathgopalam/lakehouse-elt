"""Gold layer: business-level aggregates rebuilt from silver."""

from __future__ import annotations

import duckdb

from .config import Paths, quote_path


def _tables(silver: str) -> dict[str, str]:
    live = f"FROM read_parquet('{silver}') WHERE status <> 'cancelled'"
    return {
        "daily_revenue": f"""
            SELECT order_date, COUNT(*) AS orders, SUM(quantity) AS units, SUM(order_total) AS revenue
            {live} GROUP BY order_date ORDER BY order_date""",
        "top_products": f"""
            SELECT product_id, SUM(quantity) AS units, SUM(order_total) AS revenue
            {live} GROUP BY product_id ORDER BY revenue DESC, product_id""",
        "customer_summary": f"""
            SELECT customer_id, COUNT(*) AS orders, SUM(order_total) AS lifetime_value,
                   MIN(order_date) AS first_order, MAX(order_date) AS last_order
            {live} GROUP BY customer_id ORDER BY lifetime_value DESC, customer_id""",
        "status_funnel": f"""
            SELECT status, COUNT(*) AS orders
            FROM read_parquet('{silver}') GROUP BY status ORDER BY orders DESC, status""",
    }


def build_gold(con: duckdb.DuckDBPyConnection, paths: Paths) -> dict:
    paths.gold.mkdir(parents=True, exist_ok=True)
    counts = {}
    for name, sql in _tables(quote_path(paths.silver)).items():
        target = paths.gold_table(name)
        con.execute(f"COPY ({sql}) TO '{quote_path(target)}' (FORMAT PARQUET)")
        counts[name] = con.execute(f"SELECT COUNT(*) FROM read_parquet('{quote_path(target)}')").fetchone()[0]
    return {"tables": counts}
