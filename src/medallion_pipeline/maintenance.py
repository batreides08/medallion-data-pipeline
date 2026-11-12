"""Retention for the published Delta tables.

Every publish adds a Delta version, and the files of older versions stay on disk so that they can be
read back (time travel). ``vacuum_lake`` deletes the files that no version newer than the retention
window still needs, which bounds disk use at the cost of time travel beyond that window.
"""

import logging
from dataclasses import dataclass
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from medallion_pipeline.config import LakePaths
from medallion_pipeline.tables import is_table

logger = logging.getLogger(__name__)

# Delta's own default: one week of history stays readable.
DEFAULT_RETENTION_HOURS = 168
_RETENTION_CHECK = "spark.databricks.delta.retentionDurationCheck.enabled"


@dataclass(frozen=True)
class VacuumResult:
    table: str
    files_before: int
    files_after: int
    bytes_before: int
    bytes_after: int

    @property
    def files_removed(self) -> int:
        return self.files_before - self.files_after

    @property
    def bytes_removed(self) -> int:
        return self.bytes_before - self.bytes_after


def _data_files(table: Path) -> list[Path]:
    """Parquet data files of a table, excluding the transaction log."""
    log = table / "_delta_log"
    return [p for p in table.rglob("*.parquet") if log not in p.parents]


def published_tables(paths: LakePaths) -> list[Path]:
    """Every Delta table in Silver, Quarantine and Gold. Bronze is Parquet and not vacuumed."""
    tables: list[Path] = []
    for layer in (paths.silver, paths.quarantine, paths.gold):
        if layer.is_dir():
            tables += sorted(p for p in layer.iterdir() if p.is_dir() and is_table(p))
    return tables


def vacuum_table(spark: SparkSession, table: Path, retain_hours: float) -> VacuumResult:
    before = _data_files(table)
    bytes_before = sum(p.stat().st_size for p in before)
    DeltaTable.forPath(spark, table.as_posix()).vacuum(retain_hours)
    after = _data_files(table)
    return VacuumResult(
        table=f"{table.parent.name}.{table.name}",
        files_before=len(before),
        files_after=len(after),
        bytes_before=bytes_before,
        bytes_after=sum(p.stat().st_size for p in after),
    )


def vacuum_lake(
    spark: SparkSession,
    paths: LakePaths,
    retain_hours: float = DEFAULT_RETENTION_HOURS,
    allow_short_retention: bool = False,
) -> list[VacuumResult]:
    """Delete data files that no table version inside the retention window still references.

    The latest version of every table is never affected. Versions older than ``retain_hours`` can
    no longer be read after this call.

    Delta refuses a window shorter than its default of 168 hours, because a reader or writer that
    started before the vacuum may still need the deleted files. ``allow_short_retention`` lifts
    that safeguard for this call only; use it only when nothing else is reading the lake.
    """
    if retain_hours < 0:
        raise ValueError("retain_hours must not be negative")
    if retain_hours < DEFAULT_RETENTION_HOURS and not allow_short_retention:
        raise ValueError(
            f"a retention of {retain_hours} hours is below the {DEFAULT_RETENTION_HOURS}-hour "
            "safety window; pass allow_short_retention=True if nothing else reads the lake"
        )

    previous = spark.conf.get(_RETENTION_CHECK, "true")
    if allow_short_retention:
        spark.conf.set(_RETENTION_CHECK, "false")
    try:
        results = [vacuum_table(spark, table, retain_hours) for table in published_tables(paths)]
    finally:
        spark.conf.set(_RETENTION_CHECK, previous)

    for result in results:
        logger.info(
            "Vacuum %s: removed %d of %d files (%d bytes)",
            result.table,
            result.files_removed,
            result.files_before,
            result.bytes_removed,
        )
    return results
