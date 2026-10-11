"""Data-quality gates. Failing a gate stops downstream tasks instead of publishing bad data."""

from __future__ import annotations

from dataclasses import dataclass

import duckdb

from .config import VALID_STATUSES, Paths, quote_path

_STATUS_LIST = ", ".join(f"'{s}'" for s in VALID_STATUSES)


class QualityError(Exception):
    """Raised when a quality gate fails. Deterministic, so it is never retried."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def _scalar(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def run_quality_checks(con: duckdb.DuckDBPyConnection, paths: Paths) -> list[CheckResult]:
    if not paths.silver.exists():
        raise FileNotFoundError(f"Silver table not found: {paths.silver}")
    src = f"read_parquet('{quote_path(paths.silver)}')"

    total = _scalar(con, f"SELECT COUNT(*) FROM {src}")
    dupes = _scalar(con, f"SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM {src}")
    nulls = _scalar(
        con,
        f"""SELECT COUNT(*) FROM {src} WHERE order_id IS NULL OR customer_id IS NULL
            OR product_id IS NULL OR quantity IS NULL OR unit_price IS NULL
            OR status IS NULL OR event_ts IS NULL""",
    )
    bad_qty = _scalar(con, f"SELECT COUNT(*) FROM {src} WHERE quantity <= 0")
    bad_status = _scalar(con, f"SELECT COUNT(*) FROM {src} WHERE status NOT IN ({_STATUS_LIST})")
    bad_total = _scalar(con, f"SELECT COUNT(*) FROM {src} WHERE order_total <> quantity * unit_price")

    return [
        CheckResult("has_rows", total > 0, f"{total} rows"),
        CheckResult("unique_order_id", dupes == 0, f"{dupes} duplicate ids"),
        CheckResult("no_null_required_fields", nulls == 0, f"{nulls} rows with nulls"),
        CheckResult("positive_quantity", bad_qty == 0, f"{bad_qty} rows"),
        CheckResult("valid_status", bad_status == 0, f"{bad_status} rows"),
        CheckResult("total_equals_qty_times_price", bad_total == 0, f"{bad_total} rows"),
    ]


def reconcile_gold(con: duckdb.DuckDBPyConnection, paths: Paths) -> dict:
    """Gold revenue must equal silver revenue (excluding cancelled orders)."""
    silver_rev = _scalar(
        con,
        f"SELECT COALESCE(SUM(order_total), 0) FROM read_parquet('{quote_path(paths.silver)}') "
        f"WHERE status <> 'cancelled'",
    )
    gold_rev = _scalar(
        con,
        f"SELECT COALESCE(SUM(revenue), 0) FROM read_parquet('{quote_path(paths.gold_table('daily_revenue'))}')",
    )
    if silver_rev != gold_rev:
        raise QualityError(f"Gold revenue {gold_rev} does not match silver revenue {silver_rev}")
    return {"revenue": str(gold_rev)}
