"""Run history: one JSON line per run, used to spot rejection rates that drift from normal.

The absolute limit in the quality gate catches a broken delivery. The history catches a quieter
problem: a rate that is still under the limit but far above what this pipeline usually produces.
"""

import json
import statistics
from datetime import UTC, datetime
from pathlib import Path

from medallion_pipeline.config import LakePaths
from medallion_pipeline.quality import CheckResult

HISTORY_FILE = "run_history.jsonl"
MIN_HISTORY_RUNS = 3  # fewer prior runs make a baseline too noisy to judge against
HISTORY_WINDOW = 20
DEFAULT_TREND_MARGIN = 0.05  # allowed increase over the median of earlier runs


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
) -> None:
    """Record a run, replacing an earlier entry for the same batch so replays stay idempotent."""
    runs = [run for run in read_history(paths) if run.get("batch_id") != batch_id]
    runs.append(
        {
            "batch_id": batch_id,
            "recorded_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "passed": passed,
            "row_counts": row_counts,
            "batch_rejection_rates": rates,
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
