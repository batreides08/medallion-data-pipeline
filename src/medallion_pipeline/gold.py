"""Gold layer: business marts defined in plain SQL under ``sql/gold``."""

from importlib import resources

from pyspark.sql import DataFrame

from medallion_pipeline.config import REVENUE_STATUSES, LakePaths
from medallion_pipeline.silver import SilverTables

MARTS = ("daily_revenue", "customer_lifetime_value", "product_performance")


def load_query(mart: str) -> str:
    """Return the SQL of ``mart`` with the revenue statuses filled in."""
    sql = resources.files("medallion_pipeline").joinpath(f"sql/gold/{mart}.sql")
    statuses = ", ".join(f"'{status}'" for status in REVENUE_STATUSES)
    return sql.read_text(encoding="utf-8").replace("{{ revenue_statuses }}", statuses)


def build_gold(silver: SilverTables) -> dict[str, DataFrame]:
    """Register Silver as temporary views and run each mart's query."""
    spark = silver.orders.sparkSession
    for name, df in silver.as_dict().items():
        df.createOrReplaceTempView(f"silver_{name}")
    return {mart: spark.sql(load_query(mart)) for mart in MARTS}


def write_gold(gold: dict[str, DataFrame], paths: LakePaths) -> None:
    for name, df in gold.items():
        df.write.mode("overwrite").parquet((paths.gold / name).as_posix())
