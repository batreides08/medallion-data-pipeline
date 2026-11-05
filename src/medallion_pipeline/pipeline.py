"""Orchestrates one run: raw -> Bronze -> Silver -> Gold -> quality gate."""

import json
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

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
    write_silver_merged,
    write_watermark,
)
from medallion_pipeline.quality import (
    CheckResult,
    assert_all_passed,
    check_rejection_rates,
    check_scd2,
    rejection_rates,
    run_checks,
)
from medallion_pipeline.scd2 import build_dim_customer, orders_with_customer_version, write_scd2
from medallion_pipeline.silver import SilverTables, build_silver, write_silver

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunResult:
    batch_id: str
    row_counts: dict[str, int]
    batch_rejection_rates: dict[str, float]
    max_rejection_rate: float
    checks: list[CheckResult]
    report_path: Path


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
    }
    result.report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")


def run_pipeline(
    spark: SparkSession,
    paths: LakePaths,
    batch_id: str | None = None,
    max_rejection_rate: float = DEFAULT_MAX_REJECTION_RATE,
    incremental: bool = False,
) -> RunResult:
    """Run every layer, write the report, then fail if the quality gate did not pass.

    Silver is rebuilt from all Bronze batches, so a delivery that repeats earlier records does not
    create duplicates. An entity whose share of quarantined rows in this batch exceeds
    ``max_rejection_rate`` fails the quality gate.

    With ``incremental=True`` only Bronze batches missing from the watermark (plus ``batch_id``
    itself, so a replay is merged again) are cleaned and merged into the existing Silver tables.
    The first incremental run, which has no watermark, performs a full rebuild.
    """
    batch_id = batch_id or default_batch_id()

    logger.info("Bronze: ingesting batch %s", batch_id)
    write_bronze(read_raw(spark, paths, batch_id), paths)

    bronze = read_bronze(spark, paths)
    all_batches = bronze_batches(bronze)
    processed = read_watermark(paths)
    if incremental and processed:
        new_batches = (all_batches - processed) | {batch_id}
        logger.info("Silver: merging batches %s", sorted(new_batches))
        write_silver_merged(merge_silver(read_silver(spark, paths), bronze, new_batches), paths)
    else:
        logger.info("Silver: validating and deduplicating all batches")
        write_silver(build_silver(bronze), paths)
    write_watermark(paths, all_batches)
    silver = read_silver(spark, paths)

    logger.info("Gold: building marts")
    write_gold(build_gold(silver), paths)
    logger.info("Gold: building customer history (SCD Type 2)")
    dim_customer = build_dim_customer(bronze["customers"])
    order_versions = orders_with_customer_version(silver.orders, dim_customer)
    write_scd2(dim_customer, order_versions, paths)
    gold = {
        path.name: spark.read.parquet(path.as_posix())
        for path in sorted(paths.gold.iterdir())
        if path.is_dir()
    }

    logger.info("Quality: running checks")
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
    assert_all_passed(result.checks)
    logger.info("Run %s finished; report at %s", batch_id, result.report_path)
    return result
