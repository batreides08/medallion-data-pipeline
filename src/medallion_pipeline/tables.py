"""Delta Lake tables for Silver, Quarantine and Gold.

Delta commits every write through a transaction log, so a reader sees either the old table or the
new one and never a half-written directory. Each commit is also a numbered version that can be read
back later (time travel). Bronze stays plain Parquet: it is an append-only copy of the raw files.
"""

from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def write_table(df: DataFrame, path: Path) -> None:
    """Replace the table at ``path`` in one transaction, creating it if needed."""
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        path.as_posix()
    )


def read_table(spark: SparkSession, path: Path, version: int | None = None) -> DataFrame:
    """Read the latest version of a table, or ``version`` when given."""
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path.as_posix())


def is_table(path: Path) -> bool:
    return (path / "_delta_log").is_dir()


def table_versions(spark: SparkSession, path: Path) -> list[int]:
    """Committed versions of a table, oldest first."""
    history = DeltaTable.forPath(spark, path.as_posix()).history().select("version").collect()
    return sorted(row["version"] for row in history)


def copy_table(spark: SparkSession, source: Path, target: Path) -> None:
    """Publish ``source`` as the new version of ``target`` in a single commit."""
    write_table(read_table(spark, source), target)
