"""Bronze layer: raw data, as delivered, plus lineage metadata.

Every field is read as a string so that a bad value can never fail ingestion. Judging the data is
Silver's job; Bronze only guarantees that what arrived can be replayed.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from medallion_pipeline.config import LakePaths

CORRUPT_RECORD = "_corrupt_record"
TABLES = ("customers", "products", "orders")


def _strings(*names: str) -> list[StructField]:
    return [StructField(name, StringType()) for name in names]


CUSTOMERS_SCHEMA = StructType(
    _strings("customer_id", "full_name", "email", "country", "updated_at")
)
PRODUCTS_SCHEMA = StructType(_strings("product_id", "name", "category", "list_price", "updated_at"))
ORDERS_SCHEMA = StructType(
    [
        *_strings("order_id", "customer_id", "status", "order_ts"),
        StructField(
            "items", ArrayType(StructType(_strings("product_id", "quantity", "unit_price")))
        ),
        StructField(CORRUPT_RECORD, StringType()),
    ]
)


def _with_lineage(df: DataFrame, batch_id: str) -> DataFrame:
    return df.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(batch_id).alias("_batch_id"),
    )


def read_raw(spark: SparkSession, paths: LakePaths, batch_id: str) -> dict[str, DataFrame]:
    """Read the raw files of one delivery, tagged with lineage columns."""
    customers = spark.read.schema(CUSTOMERS_SCHEMA).csv(
        (paths.raw / "customers.csv").as_posix(), header=True
    )
    products = spark.read.schema(PRODUCTS_SCHEMA).csv(
        (paths.raw / "products.csv").as_posix(), header=True
    )
    orders = spark.read.schema(ORDERS_SCHEMA).json(
        (paths.raw / "orders.jsonl").as_posix(),
        mode="PERMISSIVE",
        columnNameOfCorruptRecord=CORRUPT_RECORD,
    )
    frames = {"customers": customers, "products": products, "orders": orders}
    return {name: _with_lineage(df, batch_id) for name, df in frames.items()}


def write_bronze(tables: dict[str, DataFrame], paths: LakePaths) -> None:
    """Persist a delivery; dynamic overwrite makes re-running a batch idempotent."""
    for name, df in tables.items():
        df.write.mode("overwrite").partitionBy("_batch_id").parquet(
            (paths.bronze / name).as_posix()
        )


def read_bronze(spark: SparkSession, paths: LakePaths) -> dict[str, DataFrame]:
    """Read every batch stored in Bronze."""
    return {name: spark.read.parquet((paths.bronze / name).as_posix()) for name in TABLES}
