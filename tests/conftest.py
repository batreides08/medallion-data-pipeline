import os
import sys
from collections.abc import Iterator
from datetime import datetime

import pytest
from pyspark.sql import DataFrame, SparkSession

from medallion_pipeline.config import LakePaths
from medallion_pipeline.data_generator import ExpectedCounts, generate_raw_data
from medallion_pipeline.spark import create_spark_session


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    # Hadoop needs winutils.exe to write files on Windows. It is a third-party binary, so these
    # tests are skipped there and always run on the Linux CI runners.
    if sys.platform != "win32" or os.environ.get("HADOOP_HOME"):
        return
    skip = pytest.mark.skip(reason="writing Parquet on Windows requires HADOOP_HOME and winutils")
    for item in items:
        if "parquet_io" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = create_spark_session(app_name="medallion-tests", master="local[2]")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def lake(tmp_path_factory: pytest.TempPathFactory) -> LakePaths:
    return LakePaths(tmp_path_factory.mktemp("lake"))


@pytest.fixture(scope="session")
def expected(lake: LakePaths) -> ExpectedCounts:
    return generate_raw_data(lake.raw)


def bronze_frame(
    spark: SparkSession, columns: list[str], rows: list[tuple], schema: str | None = None
) -> DataFrame:
    """Build a Bronze-shaped frame: the given columns plus fixed lineage columns."""
    lineage_columns = ["_source_file", "_ingested_at", "_batch_id"]
    lineage_values = ("raw/file", datetime(2026, 1, 1), "batch-1")
    data = [row + lineage_values for row in rows]
    if schema:
        schema = f"{schema}, _source_file string, _ingested_at timestamp, _batch_id string"
        return spark.createDataFrame(data, schema)
    return spark.createDataFrame(data, columns + lineage_columns)
