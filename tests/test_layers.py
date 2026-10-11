import pytest

from conftest import order, write_landing
from lakehouse.bronze import ingest_bronze
from lakehouse.gold import build_gold
from lakehouse.quality import QualityError, reconcile_gold, run_quality_checks
from lakehouse.silver import build_silver
from lakehouse.state import load_state


def silver_rows(con, paths, cols="order_id, status, order_date, order_total"):
    return con.execute(
        f"SELECT {cols} FROM read_parquet('{paths.silver}') ORDER BY order_id"
    ).fetchall()


# ---- bronze ---------------------------------------------------------------

def test_bronze_ingests_once(con, paths):
    write_landing(paths, "2026-10-01", [order("A"), order("B")])
    write_landing(paths, "2026-10-02", [order("C")])
    first = ingest_bronze(con, paths)
    assert first["ingested"] == ["2026-10-01", "2026-10-02"] and first["rows"] == 3
    second = ingest_bronze(con, paths)
    assert second["ingested"] == [] and second["skipped"] == ["2026-10-01", "2026-10-02"]


def test_bronze_force_reingests(con, paths):
    write_landing(paths, "2026-10-01", [order("A")])
    ingest_bronze(con, paths)
    assert ingest_bronze(con, paths, force=True)["ingested"] == ["2026-10-01"]


def test_bronze_counts_malformed_lines(con, paths):
    write_landing(paths, "2026-10-01", [order("A")], raw_lines=["{bad json", "[1, 2]"])
    result = ingest_bronze(con, paths)
    assert result["malformed_lines"] == 2 and result["rows"] == 1


# ---- silver ---------------------------------------------------------------

def test_silver_casts_and_normalizes(con, paths):
    write_landing(paths, "2026-10-01", [
        order("A", status=" SHIPPED ", quantity="3", unit_price=9.99),
    ])
    ingest_bronze(con, paths)
    stats = build_silver(con, paths)
    assert stats["valid"] == 1 and stats["rejected"] == 0
    row = con.execute(
        f"SELECT status, quantity, unit_price, order_total FROM read_parquet('{paths.silver}')"
    ).fetchone()
    assert row[0] == "shipped" and row[1] == 3
    assert float(row[2]) == 9.99 and float(row[3]) == pytest.approx(29.97)


def test_silver_quarantines_bad_rows_with_reason(con, paths):
    write_landing(paths, "2026-10-01", [
        order("A"),
        order("B", quantity=-1),
        order("C", customer_id=None),
        order("D", status="weird"),
        order("E", ts="not-a-timestamp"),
    ])
    ingest_bronze(con, paths)
    stats = build_silver(con, paths)
    assert stats["valid"] == 1 and stats["rejected"] == 4
    reasons = dict(con.execute(
        f"SELECT order_id, reject_reason FROM read_parquet('{paths.quarantine}')").fetchall())
    assert reasons == {
        "B": "invalid quantity", "C": "missing customer_id",
        "D": "invalid status", "E": "invalid event_ts",
    }


def test_silver_latest_event_wins_and_keeps_first_order_date(con, paths):
    write_landing(paths, "2026-10-01", [order("A", "placed", "2026-10-01T10:00:00")])
    write_landing(paths, "2026-10-02", [order("A", "shipped", "2026-10-02T09:00:00")])
    ingest_bronze(con, paths)
    build_silver(con, paths)
    (oid, status, order_date, _), = silver_rows(con, paths)
    assert status == "shipped" and str(order_date) == "2026-10-01"


def test_silver_ignores_stale_late_arriving_event(con, paths):
    write_landing(paths, "2026-10-01", [order("A", "shipped", "2026-10-01T12:00:00")])
    ingest_bronze(con, paths)
    build_silver(con, paths)
    # An older event for the same order arrives in a later file.
    write_landing(paths, "2026-10-02", [order("A", "placed", "2026-10-01T08:00:00")])
    ingest_bronze(con, paths)
    build_silver(con, paths)
    (_, status, order_date, _), = silver_rows(con, paths)
    assert status == "shipped" and str(order_date) == "2026-10-01"


def test_silver_is_incremental_via_watermark(con, paths):
    write_landing(paths, "2026-10-01", [order("A")])
    ingest_bronze(con, paths)
    assert build_silver(con, paths)["new_partitions"] == ["2026-10-01"]
    assert load_state(paths.state_file)["silver_watermark"] == "2026-10-01"

    again = build_silver(con, paths)
    assert again["new_partitions"] == [] and again["silver_rows"] == 1

    write_landing(paths, "2026-10-02", [order("B")])
    ingest_bronze(con, paths)
    third = build_silver(con, paths)
    assert third["new_partitions"] == ["2026-10-02"] and third["silver_rows"] == 2


def test_silver_dedupes_exact_duplicates(con, paths):
    write_landing(paths, "2026-10-01", [order("A"), order("A")])
    ingest_bronze(con, paths)
    build_silver(con, paths)
    assert len(silver_rows(con, paths)) == 1


# ---- quality & gold -------------------------------------------------------

def _built(con, paths):
    write_landing(paths, "2026-10-01", [
        order("A", quantity=2, unit_price=10.00),
        order("B", quantity=1, unit_price=5.00, status="cancelled"),
        order("C", quantity=3, unit_price=4.00, customer_id="C002"),
    ])
    ingest_bronze(con, paths)
    build_silver(con, paths)


def test_quality_checks_pass_on_clean_silver(con, paths):
    _built(con, paths)
    assert all(r.passed for r in run_quality_checks(con, paths))


def test_quality_checks_detect_duplicates(con, paths):
    _built(con, paths)
    con.execute(f"CREATE TEMP TABLE s AS SELECT * FROM read_parquet('{paths.silver}')")
    con.execute("INSERT INTO s SELECT * FROM s WHERE order_id = 'A'")
    con.execute(f"COPY s TO '{paths.silver}' (FORMAT PARQUET)")
    failed = {r.name for r in run_quality_checks(con, paths) if not r.passed}
    assert failed == {"unique_order_id"}


def test_gold_excludes_cancelled_and_reconciles(con, paths):
    _built(con, paths)
    build_gold(con, paths)
    revenue = con.execute(
        f"SELECT revenue FROM read_parquet('{paths.gold_table('daily_revenue')}')").fetchone()[0]
    assert float(revenue) == 32.0  # 2*10 + 3*4, cancelled order excluded
    assert reconcile_gold(con, paths)["revenue"] == "32.00"
    funnel = dict(con.execute(
        f"SELECT status, orders FROM read_parquet('{paths.gold_table('status_funnel')}')").fetchall())
    assert funnel == {"placed": 2, "cancelled": 1}


def test_reconcile_detects_mismatch(con, paths):
    _built(con, paths)
    build_gold(con, paths)
    con.execute(
        f"COPY (SELECT order_date, orders, units, revenue + 1 AS revenue "
        f"FROM read_parquet('{paths.gold_table('daily_revenue')}')) "
        f"TO '{paths.gold_table('daily_revenue')}.tmp' (FORMAT PARQUET)")
    (paths.gold / "daily_revenue.parquet.tmp").replace(paths.gold_table("daily_revenue"))
    with pytest.raises(QualityError):
        reconcile_gold(con, paths)
