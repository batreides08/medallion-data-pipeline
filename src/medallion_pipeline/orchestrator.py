"""A minimal local orchestrator: named tasks run in order, each with its own retries.

Why not Airflow or Prefect? The pipeline is a short, linear chain that runs on one machine, and a
scheduler service would add a database and a daemon for no benefit. This module covers the parts
that matter here (one task per layer, bounded retries with backoff, per-task attempt counts) with
the standard library, and any external scheduler (cron, Airflow, GitHub Actions) can invoke the
``medallion-pipeline run`` command as a single step.
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    """How often a failing task is retried; the wait doubles after every failed attempt."""

    retries: int = 0
    base_delay_seconds: float = 2.0

    def __post_init__(self) -> None:
        if self.retries < 0:
            raise ValueError("retries must not be negative")
        if self.base_delay_seconds < 0:
            raise ValueError("base_delay_seconds must not be negative")


@dataclass(frozen=True)
class Task:
    """One unit of work. Exceptions listed in ``no_retry`` fail the run immediately."""

    name: str
    run: Callable[[], None]
    no_retry: tuple[type[Exception], ...] = ()


@dataclass(frozen=True)
class TaskRun:
    """How one task went: its outcome, attempts and the time its attempts took.

    ``duration_seconds`` adds up the attempts themselves and leaves out the backoff waits between
    them, so it measures work done rather than time spent waiting to retry.
    """

    name: str
    succeeded: bool
    attempts: int
    duration_seconds: float

    def as_dict(self) -> dict[str, str | int | float]:
        return {
            "name": self.name,
            "status": "succeeded" if self.succeeded else "failed",
            "attempts": self.attempts,
            "duration_seconds": round(self.duration_seconds, 3),
        }


def run_tasks(
    tasks: list[Task],
    policy: RetryPolicy | None = None,
    sleep: Callable[[float], None] = time.sleep,
    on_task_end: Callable[[TaskRun], None] | None = None,
    clock: Callable[[], float] = time.perf_counter,
) -> dict[str, int]:
    """Run ``tasks`` in order and return the number of attempts each one needed.

    A failing task is retried up to ``policy.retries`` times. When it still fails, or raises an
    exception in its ``no_retry`` list (such as a failed quality gate, which a retry cannot fix),
    the exception propagates and later tasks do not run. Without a ``policy`` nothing is retried.

    ``on_task_end`` receives a :class:`TaskRun` as soon as each task succeeds or finally fails,
    so the caller keeps the timings of the tasks that ran even when the chain stops early.
    """
    policy = policy or RetryPolicy()
    attempts: dict[str, int] = {}

    def finish(task: Task, succeeded: bool, elapsed: float) -> None:
        run = TaskRun(task.name, succeeded, attempts[task.name], elapsed)
        logger.log(
            logging.INFO if succeeded else logging.ERROR,
            "Task %s %s after %d attempt(s) in %.2fs",
            task.name,
            "succeeded" if succeeded else "failed",
            run.attempts,
            elapsed,
        )
        if on_task_end is not None:
            on_task_end(run)

    for task in tasks:
        elapsed = 0.0
        for attempt in range(1, policy.retries + 2):
            attempts[task.name] = attempt
            started = clock()
            try:
                task.run()
            except task.no_retry:
                finish(task, False, elapsed + clock() - started)
                raise
            except Exception:
                elapsed += clock() - started
                if attempt > policy.retries:
                    finish(task, False, elapsed)
                    raise
                delay = policy.base_delay_seconds * 2 ** (attempt - 1)
                logger.warning(
                    "Task %s failed on attempt %d; retrying in %.1fs", task.name, attempt, delay
                )
                sleep(delay)
            else:
                finish(task, True, elapsed + clock() - started)
                break
    return attempts
