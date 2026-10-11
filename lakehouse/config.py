"""Filesystem layout of the lakehouse."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

VALID_STATUSES = ("placed", "paid", "shipped", "delivered", "cancelled")


def quote_path(path: Path) -> str:
    """Escape a path for use inside a single-quoted SQL string literal."""
    return str(path).replace("'", "''")


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def landing(self) -> Path:
        return self.root / "landing"

    @property
    def bronze(self) -> Path:
        return self.root / "bronze" / "orders"

    @property
    def silver(self) -> Path:
        return self.root / "silver" / "orders.parquet"

    @property
    def quarantine(self) -> Path:
        return self.root / "silver" / "orders_rejects.parquet"

    @property
    def gold(self) -> Path:
        return self.root / "gold"

    @property
    def state_file(self) -> Path:
        return self.root / "state.json"

    def gold_table(self, name: str) -> Path:
        return self.gold / f"{name}.parquet"
