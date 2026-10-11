"""Synthetic order-event generator that writes daily JSONL files to the landing zone.

All data is synthetic and deterministic for a given seed. It deliberately includes
real-world problems the pipeline must handle:

* late status updates for orders placed the previous day (full-row snapshots)
* exact duplicate lines
* malformed JSON lines
* missing customer ids, negative quantities, numbers sent as strings
* inconsistent status casing/whitespace and unknown status values
"""

from __future__ import annotations

import json
import random
from datetime import date, datetime, timedelta
from pathlib import Path

PRODUCTS = {
    "P100": 19.99, "P101": 49.50, "P102": 5.25,
    "P103": 129.00, "P104": 15.75, "P105": 79.90,
}
UPDATE_STATUSES = ["paid", "shipped", "delivered", "cancelled"]


def order_id(day: date, i: int) -> str:
    return f"O{day:%Y%m%d}-{i:04d}"


def _base_order(day: date, i: int, seed: int) -> dict:
    rng = random.Random(f"{seed}-{day.isoformat()}-{i}")
    ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86_399))
    product = rng.choice(sorted(PRODUCTS))
    return {
        "order_id": order_id(day, i),
        "customer_id": f"C{rng.randint(1, 60):03d}",
        "product_id": product,
        "quantity": rng.randint(1, 5),
        "unit_price": PRODUCTS[product],
        "status": "placed",
        "event_ts": ts.isoformat(),
    }


def generate_day(landing: Path, day: date, n_orders: int = 200, seed: int = 7,
                 with_updates: bool = True) -> Path:
    """Write landing/dt=YYYY-MM-DD/orders.jsonl and return its path."""
    rng = random.Random(f"{seed}-{day.isoformat()}")
    lines: list[str] = []

    for i in range(n_orders):
        rec = _base_order(day, i, seed)
        roll = rng.random()
        if roll < 0.02:
            rec["customer_id"] = None
        elif roll < 0.04:
            rec["quantity"] = -1
        elif roll < 0.07:
            rec["status"] = "PLACED "
        elif roll < 0.08:
            rec["status"] = "unknown_status"
        if rng.random() < 0.03:
            rec["quantity"] = str(rec["quantity"])
        line = json.dumps(rec)
        lines.append(line)
        if rng.random() < 0.03:
            lines.append(line)  # exact duplicate
        if rng.random() < 0.01:
            lines.append("{bad json")

    if with_updates:
        prev = day - timedelta(days=1)
        for i in rng.sample(range(n_orders), int(n_orders * 0.35)):
            rec = _base_order(prev, i, seed)
            rec["status"] = rng.choice(UPDATE_STATUSES)
            rec["event_ts"] = (
                datetime.combine(day, datetime.min.time()) + timedelta(seconds=rng.randint(0, 86_399))
            ).isoformat()
            lines.append(json.dumps(rec))

    rng.shuffle(lines)
    out_dir = Path(landing) / f"dt={day.isoformat()}"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / "orders.jsonl"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return out


def generate_range(landing: Path, start: date, days: int, n_orders: int = 200, seed: int = 7) -> list[Path]:
    return [
        generate_day(landing, start + timedelta(days=d), n_orders, seed, with_updates=d > 0)
        for d in range(days)
    ]
