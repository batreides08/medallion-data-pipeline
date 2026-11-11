from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline.cli import main
from medallion_pipeline.config import LakePaths
from medallion_pipeline.maintenance import (
    _RETENTION_CHECK,
    published_tables,
    vacuum_lake,
)
from medallion_pipeline.tables import read_table, table_versions, write_table

# Spark reports a data file deleted by VACUUM with one of these messages.
_MISSING_FILE = r"(?i)not found|does not exist|FileNotFound"


def _lake_with_two_versions(spark: SparkSession, root: Path) -> LakePaths:
    """A lake with one Silver and one Gold table, each overwritten once."""
    paths = LakePaths(root)
    for table in (paths.silver / "orders", paths.gold / "daily_revenue"):
        write_table(spark.range(0, 10, numPartitions=2), table)
        write_table(spark.range(0, 5, numPartitions=2), table)
    # Bronze is plain Parquet and must not be touched.
    spark.range(3).write.parquet((paths.bronze / "orders").as_posix())
    return paths


@pytest.mark.parquet_io
def test_published_tables_lists_delta_tables_only(spark: SparkSession, tmp_path: Path) -> None:
    paths = _lake_with_two_versions(spark, tmp_path)
    (paths.gold / "not_a_table").mkdir()

    assert published_tables(paths) == [paths.silver / "orders", paths.gold / "daily_revenue"]


@pytest.mark.parquet_io
def test_vacuum_removes_old_files_and_keeps_the_latest_version(
    spark: SparkSession, tmp_path: Path
) -> None:
    paths = _lake_with_two_versions(spark, tmp_path)
    bronze_files = sorted((paths.bronze / "orders").rglob("*.parquet"))

    results = vacuum_lake(spark, paths, retain_hours=0, allow_short_retention=True)

    assert [r.table for r in results] == ["silver.orders", "gold.daily_revenue"]
    for result in results:
        assert result.files_removed > 0
        assert result.bytes_removed > 0
        assert result.files_after > 0
    for table in published_tables(paths):
        assert read_table(spark, table).count() == 5
        # The commit log is kept (vacuum adds its own entries), but version 0 cannot be read.
        assert table_versions(spark, table)[:2] == [0, 1]
        with pytest.raises(Exception, match=_MISSING_FILE):
            read_table(spark, table, version=0).collect()
    assert sorted((paths.bronze / "orders").rglob("*.parquet")) == bronze_files
    assert spark.conf.get(_RETENTION_CHECK, "true") == "true"


@pytest.mark.parquet_io
def test_default_retention_keeps_recent_versions(spark: SparkSession, tmp_path: Path) -> None:
    paths = _lake_with_two_versions(spark, tmp_path)

    results = vacuum_lake(spark, paths)

    assert all(r.files_removed == 0 for r in results)
    for table in published_tables(paths):
        assert read_table(spark, table, version=0).count() == 10


def test_short_retention_requires_explicit_permission(spark: SparkSession, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="safety window"):
        vacuum_lake(spark, LakePaths(tmp_path), retain_hours=1)
    with pytest.raises(ValueError, match="negative"):
        vacuum_lake(spark, LakePaths(tmp_path), retain_hours=-1, allow_short_retention=True)


def test_vacuum_of_an_empty_lake_does_nothing(spark: SparkSession, tmp_path: Path) -> None:
    assert vacuum_lake(spark, LakePaths(tmp_path)) == []


def test_cli_vacuum_refuses_short_retention_without_the_flag(tmp_path: Path) -> None:
    assert main(["vacuum", "--data-dir", str(tmp_path), "--retain-hours", "0"]) == 2


@pytest.mark.parametrize("value", ["-1", "soon"])
def test_cli_vacuum_rejects_invalid_retention(tmp_path: Path, value: str) -> None:
    with pytest.raises(SystemExit):
        main(["vacuum", "--data-dir", str(tmp_path), "--retain-hours", value])


@pytest.mark.parquet_io
def test_cli_vacuum_with_short_retention(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = _lake_with_two_versions(spark, tmp_path)
    monkeypatch.setattr("medallion_pipeline.cli.create_spark_session", lambda: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    args = ["vacuum", "--data-dir", str(tmp_path), "--retain-hours", "0", "--allow-short-retention"]
    assert main(args) == 0
    with pytest.raises(Exception, match=_MISSING_FILE):
        read_table(spark, paths.gold / "daily_revenue", version=0).collect()
