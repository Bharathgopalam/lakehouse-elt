"""A minimal DAG runner: dependency ordering, retries, and upstream-failure propagation.

This mirrors what an orchestrator such as Airflow does, in ~60 lines, so the concepts are
visible. See dags/lakehouse_dag.py for the same pipeline expressed as an Airflow DAG.
"""

from __future__ import annotations

import graphlib
import time
from dataclasses import dataclass, field
from typing import Any, Callable

from .quality import QualityError


@dataclass
class Task:
    name: str
    fn: Callable[[], Any]
    deps: tuple[str, ...] = ()


@dataclass
class TaskRun:
    name: str
    status: str  # success | failed | skipped
    seconds: float = 0.0
    attempts: int = 0
    error: str = ""
    result: Any = field(default=None, repr=False)


def run_dag(tasks: list[Task], retries: int = 1, retry_delay: float = 0.0,
            sleep: Callable[[float], None] = time.sleep) -> list[TaskRun]:
    by_name = {t.name: t for t in tasks}
    for t in tasks:
        for dep in t.deps:
            if dep not in by_name:
                raise ValueError(f"Task '{t.name}' depends on unknown task '{dep}'")
    try:
        order = list(graphlib.TopologicalSorter({t.name: set(t.deps) for t in tasks}).static_order())
    except graphlib.CycleError as exc:
        raise ValueError(f"Cycle detected in DAG: {exc.args[1]}") from exc

    runs: list[TaskRun] = []
    blocked: set[str] = set()
    for name in order:
        task = by_name[name]
        if any(dep in blocked for dep in task.deps):
            runs.append(TaskRun(name, "skipped", error="upstream task did not succeed"))
            blocked.add(name)
            continue

        started = time.perf_counter()
        attempts = 0
        while True:
            attempts += 1
            try:
                result = task.fn()
                runs.append(TaskRun(name, "success", time.perf_counter() - started, attempts, result=result))
                break
            except QualityError as exc:  # deterministic: retrying cannot help
                runs.append(TaskRun(name, "failed", time.perf_counter() - started, attempts, str(exc)))
                blocked.add(name)
                break
            except Exception as exc:  # noqa: BLE001 - orchestrators catch everything
                if attempts > retries:
                    runs.append(TaskRun(name, "failed", time.perf_counter() - started, attempts,
                                        f"{type(exc).__name__}: {exc}"))
                    blocked.add(name)
                    break
                sleep(retry_delay)
    return runs
