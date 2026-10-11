"""Reference Airflow DAG for the lakehouse pipeline.

This file shows how the built-in runner maps onto Airflow. It is NOT exercised by this
repo's tests (they would need apache-airflow installed). To try it, install
apache-airflow>=2.7, set LAKEHOUSE_ROOT, and copy this file into your Airflow dags folder.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
from airflow import DAG
from airflow.operators.python import PythonOperator

from lakehouse.bronze import ingest_bronze
from lakehouse.config import Paths
from lakehouse.gold import build_gold
from lakehouse.quality import QualityError, reconcile_gold, run_quality_checks
from lakehouse.silver import build_silver

PATHS = Paths(Path(os.environ.get("LAKEHOUSE_ROOT", "/opt/airflow/data")))


def _run(fn):
    con = duckdb.connect()
    try:
        return fn(con, PATHS)
    finally:
        con.close()


def _quality(con, paths):
    failed = [r for r in run_quality_checks(con, paths) if not r.passed]
    if failed:
        raise QualityError("; ".join(f"{r.name} ({r.detail})" for r in failed))


with DAG(
    dag_id="lakehouse_orders",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["lakehouse", "orders"],
) as dag:
    bronze = PythonOperator(task_id="ingest_bronze", python_callable=lambda: _run(ingest_bronze))
    silver = PythonOperator(task_id="build_silver", python_callable=lambda: _run(build_silver))
    quality = PythonOperator(task_id="quality_checks", python_callable=lambda: _run(_quality))
    gold = PythonOperator(task_id="build_gold", python_callable=lambda: _run(build_gold))
    reconcile = PythonOperator(task_id="reconcile_gold", python_callable=lambda: _run(reconcile_gold))

    bronze >> silver >> quality >> gold >> reconcile
