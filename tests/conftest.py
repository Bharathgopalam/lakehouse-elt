import json
from pathlib import Path

import duckdb
import pytest

from lakehouse.config import Paths


@pytest.fixture
def paths(tmp_path):
    return Paths(tmp_path / "lake")


@pytest.fixture
def con():
    connection = duckdb.connect()
    yield connection
    connection.close()


def order(order_id="O1", status="placed", ts="2026-10-01T10:00:00", **over):
    rec = {
        "order_id": order_id, "customer_id": "C001", "product_id": "P100",
        "quantity": 2, "unit_price": 10.00, "status": status, "event_ts": ts,
    }
    rec.update(over)
    return rec


def write_landing(paths: Paths, dt: str, records, raw_lines=()):
    """Write a landing file for a day from dicts (plus optional raw text lines)."""
    out_dir = paths.landing / f"dt={dt}"
    out_dir.mkdir(parents=True, exist_ok=True)
    lines = [json.dumps(r) for r in records] + list(raw_lines)
    (out_dir / "orders.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out_dir
