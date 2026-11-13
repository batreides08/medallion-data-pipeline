"""Orchestrates one run: raw -> Bronze -> Silver -> Gold -> quality gate."""

import json
import logging
import shutil
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion_pipeline.bronze import read_bronze, read_raw, write_bronze
from medallion_pipeline.config import DEFAULT_MAX_REJECTION_RATE, LakePaths
from medallion_pipeline.gold import build_gold, write_gold
from medallion_pipeline.history import append_run, check_rejection_trend, read_history
from medallion_pipeline.incremental import (
    bronze_batches,
    merge_silver,
    read_silver,
    read_watermark,
    write_watermark,
)
from medallion_pipeline.orchestrator import RetryPolicy, Task, TaskRun, run_tasks
from medallion_pipeline.quality import (
    CheckResult,
    DataQualityError,
    assert_all_passed,
    check_rejection_rates,
    check_scd2,
    rejection_rates,
    run_checks,
)
from medallion_pipeline.scd2 import build_dim_customer, orders_with_customer_version, write_scd2
from medallion_pipeline.silver import SilverTables, build_silver, write_silver
from medallion_pipeline.tables import copy_table, read_table

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    batch_id: str
    row_counts: dict[str, int]
    batch_rejection_rates: dict[str, float]
    max_rejection_rate: float
    checks: list[CheckResult]
    report_path: Path
    task_attempts: dict[str, int] = field(default_factory=dict)
    task_runs: list[TaskRun] = field(default_factory=list)


def default_batch_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def count_rows(silver: SilverTables, gold: dict[str, DataFrame]) -> dict[str, int]:
    counts = {f"silver.{name}": df.count() for name, df in silver.as_dict().items()}
    counts |= {f"quarantine.{name}": df.count() for name, df in silver.quarantine.items()}
    counts |= {f"gold.{name}": df.count() for name, df in gold.items()}
    return counts


def batch_row_counts(silver: SilverTables, batch_id: str) -> dict[str, int]:
    """Valid and quarantined rows per entity that came from ``batch_id``.

    Quarantine keeps every rejected row from every batch, while Silver keeps one version per key,
    so rates over the whole lake drift upwards each time data is re-delivered. Counting only the
    current batch measures the delivery that was just received.
    """
    in_batch = F.col("_batch_id") == batch_id
    counts = {
        f"silver.{name}": df.filter(in_batch).count() for name, df in silver.as_dict().items()
    }
    counts |= {
        f"quarantine.{name}": df.filter(in_batch).count() for name, df in silver.quarantine.items()
    }
    return counts


def write_report(result: RunResult) -> None:
    result.report_path.parent.mkdir(parents=True, exist_ok=True)
    report = {
        "batch_id": result.batch_id,
        "row_counts": result.row_counts,
        "batch_rejection_rates": result.batch_rejection_rates,
        "max_rejection_rate": result.max_rejection_rate,
        "checks": [check.as_dict() for check in result.checks],
        "tasks": [run.as_dict() for run in result.task_runs],
        "total_task_seconds": round(sum(run.duration_seconds for run in result.task_runs), 3),
    }
    result.report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def run_pipeline(
    spark: SparkSession,
    paths: LakePaths,
    batch_id: str | None = None,
    max_rejection_rate: float = DEFAULT_MAX_REJECTION_RATE,
    incremental: bool = False,
    retry_policy: RetryPolicy = RetryPolicy(),  # noqa: B008 - frozen, safe as a default
) -> RunResult:
    """Run every layer as its own retryable task, write the report, then enforce the gate.

    Silver is rebuilt from all Bronze batches, so a delivery that repeats earlier records does not
    create duplicates. An entity whose share of quarantined rows in this batch exceeds
    ``max_rejection_rate`` fails the quality gate.

    With ``incremental=True`` only Bronze batches missing from the watermark (plus ``batch_id``
    itself, so a replay is merged again) are cleaned and merged into the existing Silver tables.
    The first incremental run, which has no watermark, performs a full rebuild.

    Silver and Gold are first built in a candidate lake (``state/candidate``). Only when the quality
    gate passes does the ``publish`` task copy them over the live Delta tables, so a failed gate
    leaves the previously published data untouched and every earlier version stays queryable.

    Each layer (bronze, silver, gold, quality, publish) is a task retried according to
    ``retry_policy``. The report lists every task that ran with its outcome, attempts and duration,
    so a slow layer is visible run by run.
    Every task overwrites its own output, so repeating one is safe. A failed quality gate is never
    retried because running the same checks again cannot change the outcome.
    """
    batch_id = batch_id or default_batch_id()
    candidate = paths.candidate
    state: dict[str, Any] = {}

    def bronze_task() -> None:
        logger.info("Bronze: ingesting batch %s", batch_id)
        write_bronze(read_raw(spark, paths, batch_id), paths)
        state["bronze"] = read_bronze(spark, paths)
        state["all_batches"] = bronze_batches(state["bronze"])

    def silver_task() -> None:
        bronze = state["bronze"]
        processed = read_watermark(paths)
        # Built in the candidate lake: the live tables change only after the quality gate passes.
        shutil.rmtree(candidate.root, ignore_errors=True)
        if incremental and processed:
            new_batches = (state["all_batches"] - processed) | {batch_id}
            logger.info("Silver: merging batches %s", sorted(new_batches))
            write_silver(merge_silver(read_silver(spark, paths), bronze, new_batches), candidate)
        else:
            logger.info("Silver: validating and deduplicating all batches")
            write_silver(build_silver(bronze), candidate)
        state["silver"] = read_silver(spark, candidate)

    def gold_task() -> None:
        logger.info("Gold: building marts")
        write_gold(build_gold(state["silver"]), candidate)
        logger.info("Gold: building customer history (SCD Type 2)")
        dim_customer = build_dim_customer(state["bronze"]["customers"])
        order_versions = orders_with_customer_version(state["silver"].orders, dim_customer)
        write_scd2(dim_customer, order_versions, candidate)
        state["gold"] = {
            path.name: read_table(spark, path)
            for path in sorted(candidate.gold.iterdir())
            if path.is_dir()
        }

    def quality_task() -> None:
        logger.info("Quality: running checks")
        silver, gold = state["silver"], state["gold"]
        row_counts = count_rows(silver, gold)
        rates = rejection_rates(batch_row_counts(silver, batch_id))
        result = RunResult(
            batch_id=batch_id,
            row_counts=row_counts,
            batch_rejection_rates=rates,
            max_rejection_rate=max_rejection_rate,
            checks=[
                *run_checks(silver, gold),
                *check_scd2(silver, gold["dim_customer"], gold["order_customer_version"]),
                *check_rejection_rates(rates, max_rejection_rate),
                *check_rejection_trend(rates, read_history(paths)),
            ],
            report_path=paths.reports / f"run_{batch_id}.json",
        )
        write_report(result)
        append_run(paths, batch_id, row_counts, rates, all(c.passed for c in result.checks))
        state["result"] = result
        assert_all_passed(result.checks)

    def publish_task() -> None:
        """Copy the verified candidate tables over the live ones, one Delta commit per table."""
        logger.info("Publish: replacing live Silver, Quarantine and Gold")
        for source_root, live_root in (
            (candidate.silver, paths.silver),
            (candidate.quarantine, paths.quarantine),
            (candidate.gold, paths.gold),
        ):
            for table in sorted(p for p in source_root.iterdir() if p.is_dir()):
                copy_table(spark, table, live_root / table.name)
        # Last, so a crash while publishing makes the next incremental run repeat the merge.
        write_watermark(paths, state["all_batches"])
        shutil.rmtree(candidate.root, ignore_errors=True)

    task_runs: list[TaskRun] = []
    try:
        attempts = run_tasks(
            [
                Task("bronze", bronze_task),
                Task("silver", silver_task),
                Task("gold", gold_task),
                Task("quality", quality_task, no_retry=(DataQualityError,)),
                Task("publish", publish_task),
            ],
            retry_policy,
            on_task_end=task_runs.append,
        )
    finally:
        # The quality task writes the report before publish runs, so rewrite it with the timing
        # of every task that ran, including a failed gate or a failed publish.
        if "result" in state:
            state["result"] = replace(state["result"], task_runs=list(task_runs))
            write_report(state["result"])
    result = replace(state["result"], task_attempts=attempts)
    logger.info("Run %s finished; report at %s", batch_id, result.report_path)
    return result
