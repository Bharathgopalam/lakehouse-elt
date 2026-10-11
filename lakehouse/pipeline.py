"""Wires the layers into a DAG:

    ingest_bronze -> build_silver -> quality_checks -> build_gold -> reconcile_gold
"""

from __future__ import annotations

from pathlib import Path

import duckdb

from .bronze import ingest_bronze
from .config import Paths, quote_path
from .gold import build_gold
from .quality import QualityError, reconcile_gold, run_quality_checks
from .runner import Task, TaskRun, run_dag
from .silver import build_silver


def build_tasks(con: duckdb.DuckDBPyConnection, paths: Paths) -> list[Task]:
    def quality_gate() -> dict:
        results = run_quality_checks(con, paths)
        failed = [r for r in results if not r.passed]
        if failed:
            raise QualityError("; ".join(f"{r.name} ({r.detail})" for r in failed))
        return {"checks_passed": len(results)}

    return [
        Task("ingest_bronze", lambda: ingest_bronze(con, paths)),
        Task("build_silver", lambda: build_silver(con, paths), ("ingest_bronze",)),
        Task("quality_checks", quality_gate, ("build_silver",)),
        Task("build_gold", lambda: build_gold(con, paths), ("quality_checks",)),
        Task("reconcile_gold", lambda: reconcile_gold(con, paths), ("build_gold",)),
    ]


def run_pipeline(root: Path, retries: int = 1) -> list[TaskRun]:
    paths = Paths(Path(root))
    con = duckdb.connect()
    try:
        return run_dag(build_tasks(con, paths), retries=retries)
    finally:
        con.close()


def open_warehouse(paths: Paths) -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection with views over whatever layers exist, for ad-hoc SQL."""
    con = duckdb.connect()
    if paths.bronze.exists() and any(paths.bronze.glob("ingest_date=*")):
        con.execute(
            f"CREATE VIEW bronze AS SELECT * FROM "
            f"read_parquet('{quote_path(paths.bronze)}/*/data.parquet', hive_partitioning = true)"
        )
    if paths.silver.exists():
        con.execute(f"CREATE VIEW silver AS SELECT * FROM read_parquet('{quote_path(paths.silver)}')")
    if paths.quarantine.exists():
        con.execute(f"CREATE VIEW rejects AS SELECT * FROM read_parquet('{quote_path(paths.quarantine)}')")
    if paths.gold.exists():
        for table in sorted(paths.gold.glob("*.parquet")):
            con.execute(f"CREATE VIEW {table.stem} AS SELECT * FROM read_parquet('{quote_path(table)}')")
    return con
