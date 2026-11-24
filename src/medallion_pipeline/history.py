"""Run history: one JSON line per run, used to spot rejection rates and task times that drift.

The absolute limit in the quality gate catches a broken delivery. The history catches a quieter
problem: a rate that is still under the limit but far above what this pipeline usually produces.
It also keeps how long each task took, so a layer that suddenly runs much slower than usual is
flagged in the run report instead of only being visible to someone comparing reports by hand.
"""

import json
import statistics
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from medallion_pipeline.config import LakePaths
from medallion_pipeline.quality import CheckResult

HISTORY_FILE = "run_history.jsonl"
MIN_HISTORY_RUNS = 3  # fewer prior runs make a baseline too noisy to judge against
HISTORY_WINDOW = 20
DEFAULT_TREND_MARGIN = 0.05  # allowed increase over the median of earlier runs
DEFAULT_SLOWDOWN_FACTOR = 2.0  # a task this many times slower than its median is flagged
MIN_SLOWDOWN_SECONDS = 5.0  # ...but only if it is also this much slower, so tiny tasks stay quiet


def history_path(paths: LakePaths) -> Path:
    return paths.state / HISTORY_FILE


def read_history(paths: LakePaths) -> list[dict]:
    """Return recorded runs, oldest first. Unreadable lines are skipped, not fatal."""
    path = history_path(paths)
    if not path.exists():
        return []
    runs = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            runs.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return runs


def append_run(
    paths: LakePaths,
    batch_id: str,
    row_counts: dict[str, int],
    rates: dict[str, float],
    passed: bool,
    task_seconds: dict[str, float] | None = None,
) -> None:
    """Record a run, replacing an earlier entry for the same batch so replays stay idempotent.

    ``task_seconds`` maps each task that succeeded to the seconds its attempts took.
    """
    runs = [run for run in read_history(paths) if run.get("batch_id") != batch_id]
    runs.append(
        {
            "batch_id": batch_id,
            "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "passed": passed,
            "row_counts": row_counts,
            "batch_rejection_rates": rates,
            "task_seconds": task_seconds or {},
        }
    )
    path = history_path(paths)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text("".join(json.dumps(run) + "\n" for run in runs), encoding="utf-8")
    tmp.replace(path)


def check_rejection_trend(
    rates: dict[str, float],
    history: list[dict],
    margin: float = DEFAULT_TREND_MARGIN,
) -> list[CheckResult]:
    """Fail an entity whose rate exceeds the median of recent passing runs by more than ``margin``.

    Only runs that passed count as the baseline, so one bad delivery does not make the next bad
    one look normal. With fewer than ``MIN_HISTORY_RUNS`` baseline runs nothing is judged.
    """
    if margin < 0:
        raise ValueError(f"margin must not be negative, got {margin}")
    baseline = [run for run in history if run.get("passed")][-HISTORY_WINDOW:]
    results = []
    for entity, rate in rates.items():
        name = f"rejection_trend:{entity}"
        past = [
            run["batch_rejection_rates"][entity]
            for run in baseline
            if entity in run.get("batch_rejection_rates", {})
        ]
        if len(past) < MIN_HISTORY_RUNS:
            results.append(
                CheckResult(
                    name, True, f"skipped: {len(past)} prior runs (need {MIN_HISTORY_RUNS})"
                )
            )
            continue
        typical = statistics.median(past)
        results.append(
            CheckResult(
                name,
                rate <= typical + margin,
                f"{rate:.2%} vs median {typical:.2%} of {len(past)} runs (margin {margin:.2%})",
            )
        )
    return results


@dataclass(frozen=True)
class SlowTask:
    """A task that took much longer than it usually does in passing runs."""

    name: str
    seconds: float
    median_seconds: float
    baseline_runs: int

    def as_dict(self) -> dict[str, str | int | float]:
        return {
            "name": self.name,
            "seconds": round(self.seconds, 3),
            "median_seconds": round(self.median_seconds, 3),
            "baseline_runs": self.baseline_runs,
        }


def find_slow_tasks(
    task_seconds: dict[str, float],
    history: list[dict],
    factor: float = DEFAULT_SLOWDOWN_FACTOR,
    min_seconds: float = MIN_SLOWDOWN_SECONDS,
) -> list[SlowTask]:
    """Return the tasks that took more than ``factor`` times their median in recent passing runs.

    A task must also be at least ``min_seconds`` slower than its median, because a task that
    normally takes a fraction of a second doubles from noise alone. Runs recorded before task
    times were kept, and tasks with fewer than ``MIN_HISTORY_RUNS`` timings, are not judged.
    """
    if factor < 1:
        raise ValueError(f"factor must be at least 1, got {factor}")
    if min_seconds < 0:
        raise ValueError(f"min_seconds must not be negative, got {min_seconds}")
    baseline = [run for run in history if run.get("passed")][-HISTORY_WINDOW:]
    slow = []
    for name, seconds in task_seconds.items():
        past = [
            run["task_seconds"][name] for run in baseline if name in run.get("task_seconds", {})
        ]
        if len(past) < MIN_HISTORY_RUNS:
            continue
        typical = statistics.median(past)
        if seconds > typical * factor and seconds - typical >= min_seconds:
            slow.append(SlowTask(name, seconds, typical, len(past)))
    return slow


def format_history(runs: Sequence[dict], last: int | None = None) -> str:
    """Render recorded runs, newest last, as a plain-text table for the ``history`` command."""
    shown = list(runs[-last:] if last else runs)
    header = ("batch_id", "recorded_at", "gate", "total_s", "max_rejection", "slowest_task")
    rows = [header]
    for run in shown:
        seconds = run.get("task_seconds") or {}
        rates = run.get("batch_rejection_rates") or {}
        worst = max(rates.items(), key=lambda item: item[1], default=None)
        slowest = max(seconds.items(), key=lambda item: item[1], default=None)
        rows.append(
            (
                str(run.get("batch_id", "?")),
                str(run.get("recorded_at", "?")),
                "passed" if run.get("passed") else "failed",
                f"{sum(seconds.values()):.1f}" if seconds else "-",
                f"{worst[0]} {worst[1]:.2%}" if worst else "-",
                f"{slowest[0]} {slowest[1]:.1f}s" if slowest else "-",
            )
        )
    widths = [max(len(row[i]) for row in rows) for i in range(len(header))]
    return "\n".join(
        "  ".join(cell.ljust(width) for cell, width in zip(row, widths, strict=True)).rstrip()
        for row in rows
    )
