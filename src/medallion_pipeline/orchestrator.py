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


def run_tasks(
    tasks: list[Task],
    policy: RetryPolicy = RetryPolicy(),  # noqa: B008 - frozen, safe as a default
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, int]:
    """Run ``tasks`` in order and return the number of attempts each one needed.

    A failing task is retried up to ``policy.retries`` times. When it still fails, or raises an
    exception in its ``no_retry`` list (such as a failed quality gate, which a retry cannot fix),
    the exception propagates and later tasks do not run.
    """
    attempts: dict[str, int] = {}
    for task in tasks:
        for attempt in range(1, policy.retries + 2):
            attempts[task.name] = attempt
            try:
                task.run()
                break
            except task.no_retry:
                raise
            except Exception:
                if attempt > policy.retries:
                    logger.error("Task %s failed after %d attempt(s)", task.name, attempt)
                    raise
                delay = policy.base_delay_seconds * 2 ** (attempt - 1)
                logger.warning(
                    "Task %s failed on attempt %d; retrying in %.1fs", task.name, attempt, delay
                )
                sleep(delay)
    return attempts
