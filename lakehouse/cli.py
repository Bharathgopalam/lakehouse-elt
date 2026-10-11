"""Command-line interface: generate sample data, run the pipeline, query the warehouse."""

from __future__ import annotations

import argparse
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional, Sequence

import duckdb

from .config import Paths
from .generate import generate_range
from .pipeline import open_warehouse, run_pipeline


def cmd_generate(args) -> int:
    start = date.fromisoformat(args.start) if args.start else date.today() - timedelta(days=args.days - 1)
    files = generate_range(Paths(Path(args.root)).landing, start, args.days, args.orders, args.seed)
    print(f"Generated {len(files)} landing files starting {start}.")
    return 0


def cmd_run(args) -> int:
    runs = run_pipeline(Path(args.root), retries=args.retries)
    for r in runs:
        line = f"{r.status:<8} {r.name:<16} {r.seconds:5.2f}s  attempts={r.attempts}"
        if r.error:
            line += f"  error: {r.error}"
        print(line)
        if r.result and r.status == "success":
            print(f"           {r.result}")
    return 0 if all(r.status == "success" for r in runs) else 1


def cmd_query(args) -> int:
    con = open_warehouse(Paths(Path(args.root)))
    try:
        cur = con.execute(args.sql)
    except duckdb.Error as exc:
        print(f"SQL error: {exc}", file=sys.stderr)
        return 1
    cols = [d[0] for d in cur.description]
    print("  ".join(cols))
    for row in cur.fetchall():
        print("  ".join(str(v) for v in row))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="lakehouse", description="Medallion ELT pipeline on DuckDB")
    p.add_argument("--root", default="data", help="Lakehouse root directory (default: ./data)")
    sub = p.add_subparsers(dest="command", required=True)

    g = sub.add_parser("generate", help="Generate synthetic landing files")
    g.add_argument("--days", type=int, default=5)
    g.add_argument("--orders", type=int, default=200)
    g.add_argument("--start", help="First day, YYYY-MM-DD (default: days ago)")
    g.add_argument("--seed", type=int, default=7)
    g.set_defaults(func=cmd_generate)

    r = sub.add_parser("run", help="Run bronze -> silver -> quality -> gold")
    r.add_argument("--retries", type=int, default=1)
    r.set_defaults(func=cmd_run)

    q = sub.add_parser("query", help="Run SQL over bronze/silver/rejects/gold views")
    q.add_argument("sql")
    q.set_defaults(func=cmd_query)
    return p


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
