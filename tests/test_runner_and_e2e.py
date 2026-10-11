from datetime import date

import pytest

from conftest import order, write_landing
from lakehouse.config import Paths
from lakehouse.generate import generate_range
from lakehouse.pipeline import open_warehouse, run_pipeline
from lakehouse.quality import QualityError
from lakehouse.runner import Task, run_dag


# ---- DAG runner -----------------------------------------------------------

def test_runs_in_dependency_order():
    calls = []
    tasks = [
        Task("c", lambda: calls.append("c"), ("a", "b")),
        Task("b", lambda: calls.append("b"), ("a",)),
        Task("a", lambda: calls.append("a")),
    ]
    runs = run_dag(tasks)
    assert calls == ["a", "b", "c"]
    assert all(r.status == "success" for r in runs)


def test_failure_skips_downstream_but_not_independent_tasks():
    def boom():
        raise RuntimeError("nope")

    tasks = [
        Task("a", lambda: 1),
        Task("b", boom, ("a",)),
        Task("c", lambda: 2, ("b",)),
        Task("d", lambda: 3, ("a",)),
    ]
    status = {r.name: r.status for r in run_dag(tasks, retries=0)}
    assert status == {"a": "success", "b": "failed", "c": "skipped", "d": "success"}


def test_retries_transient_errors_then_succeeds():
    attempts = {"n": 0}

    def flaky():
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise ConnectionError("temporary")
        return "ok"

    (run,) = run_dag([Task("flaky", flaky)], retries=2, sleep=lambda s: None)
    assert run.status == "success" and run.attempts == 3


def test_quality_errors_are_not_retried():
    attempts = {"n": 0}

    def gate():
        attempts["n"] += 1
        raise QualityError("bad data")

    (run,) = run_dag([Task("gate", gate)], retries=5, sleep=lambda s: None)
    assert run.status == "failed" and attempts["n"] == 1


def test_unknown_dependency_and_cycle_rejected():
    with pytest.raises(ValueError, match="unknown task"):
        run_dag([Task("a", lambda: 1, ("missing",))])
    with pytest.raises(ValueError, match="Cycle"):
        run_dag([Task("a", lambda: 1, ("b",)), Task("b", lambda: 1, ("a",))])


# ---- end to end -----------------------------------------------------------

def test_generator_is_deterministic(tmp_path):
    a = generate_range(tmp_path / "a", date(2026, 10, 1), 2, 50, seed=1)
    b = generate_range(tmp_path / "b", date(2026, 10, 1), 2, 50, seed=1)
    assert [p.read_text() for p in a] == [p.read_text() for p in b]


def test_full_pipeline_on_generated_data(tmp_path):
    root = tmp_path / "lake"
    generate_range(root / "landing", date(2026, 10, 1), 4, 150, seed=3)
    runs = run_pipeline(root)
    assert [r.status for r in runs] == ["success"] * 5

    con = open_warehouse(Paths(root))
    silver = con.execute("SELECT COUNT(*) FROM silver").fetchone()[0]
    rejected = con.execute("SELECT COUNT(*) FROM rejects").fetchone()[0]
    assert silver > 0 and rejected > 0
    # Status updates arrive for earlier days, so some orders must have moved past 'placed'.
    assert con.execute("SELECT COUNT(*) FROM silver WHERE status <> 'placed'").fetchone()[0] > 0
    assert con.execute("SELECT COUNT(*) - COUNT(DISTINCT order_id) FROM silver").fetchone()[0] == 0
    con.close()


def test_pipeline_is_idempotent_and_picks_up_new_days(tmp_path):
    root = tmp_path / "lake"
    generate_range(root / "landing", date(2026, 10, 1), 2, 100, seed=5)
    run_pipeline(root)

    con = open_warehouse(Paths(root))
    before = con.execute("SELECT COUNT(*), SUM(order_total) FROM silver").fetchone()
    con.close()

    run_pipeline(root)  # nothing new: results unchanged
    con = open_warehouse(Paths(root))
    assert con.execute("SELECT COUNT(*), SUM(order_total) FROM silver").fetchone() == before
    con.close()

    generate_range(root / "landing", date(2026, 10, 1), 3, 100, seed=5)  # adds day 3
    runs = run_pipeline(root)
    assert all(r.status == "success" for r in runs)
    con = open_warehouse(Paths(root))
    assert con.execute("SELECT COUNT(*) FROM silver").fetchone()[0] > before[0]
    con.close()


def test_pipeline_blocks_gold_when_quality_gate_fails(tmp_path):
    root = tmp_path / "lake"
    # Every row is invalid, so silver ends up empty and the has_rows gate fails.
    write_landing(Paths(root), "2026-10-01", [order("A", quantity=-5)])
    runs = {r.name: r.status for r in run_pipeline(root)}
    assert runs["build_silver"] == "success"
    assert runs["quality_checks"] == "failed"
    assert runs["build_gold"] == "skipped" and runs["reconcile_gold"] == "skipped"
