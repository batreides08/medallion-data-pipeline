"""Spark session factory."""

import os
import sys

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str = "medallion-pipeline", master: str = "local[*]"
) -> SparkSession:
    """Build a local session whose settings make runs reproducible."""
    # Workers must use this interpreter; a bare "python3" does not exist on Windows.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        # Re-running a batch replaces only that batch's Bronze partition.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # Batch ids stay strings even when they look numeric.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        # Delta Lake gives Silver and Gold atomic writes and time travel.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()
