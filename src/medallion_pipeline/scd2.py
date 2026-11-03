"""Customer history as a Slowly Changing Dimension Type 2.

Silver keeps only the newest customer version. This module rebuilds the full history from the
valid Bronze versions: each row of ``dim_customer`` is one version of a customer with the range
``[valid_from, valid_to)`` in which it was current. Orders are then joined to the version that was
valid when they were placed.

Only attribute changes create a version: a re-delivery of identical attributes is collapsed, so the
dimension does not grow with every batch.
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from medallion_pipeline.config import LakePaths
from medallion_pipeline.silver import validate_customers

TRACKED = ("full_name", "email", "country")


def build_dim_customer(bronze_customers: DataFrame) -> DataFrame:
    """Build ``dim_customer`` from every valid customer version in Bronze.

    Columns: ``customer_key``, ``customer_id``, the tracked attributes, ``version_number``,
    ``valid_from``, ``valid_to`` (NULL while current) and ``is_current``.
    """
    versions = validate_customers(bronze_customers).valid
    # The same instant delivered twice (or twice with different data): the latest ingestion wins.
    one_per_instant = (
        versions.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("customer_id", "updated_at").orderBy(
                    F.col("_ingested_at").desc()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    by_time = Window.partitionBy("customer_id").orderBy("updated_at")
    previous = [F.lag(c).over(by_time) for c in TRACKED]
    changed = F.lit(False)
    for column, prior in zip(TRACKED, previous, strict=True):
        changed = changed | ~F.col(column).eqNullSafe(prior)
    # lag is NULL on the first row, which also counts as a change.
    changes = one_per_instant.withColumn("_changed", changed).filter(F.col("_changed"))

    by_time = Window.partitionBy("customer_id").orderBy("updated_at")
    return changes.select(
        F.concat_ws("|", "customer_id", F.date_format("updated_at", "yyyyMMddHHmmss")).alias(
            "customer_key"
        ),
        "customer_id",
        *TRACKED,
        F.row_number().over(by_time).alias("version_number"),
        F.col("updated_at").alias("valid_from"),
        F.lead("updated_at").over(by_time).alias("valid_to"),
    ).withColumn("is_current", F.col("valid_to").isNull())


def orders_with_customer_version(orders: DataFrame, dim_customer: DataFrame) -> DataFrame:
    """Attach to each order the customer version valid at ``order_ts``.

    An order placed before the first known version (a customer first seen later than the order)
    takes that first version, so no order is lost by the join.
    """
    dim = dim_customer.select(
        "customer_id",
        "customer_key",
        "version_number",
        "valid_from",
        "valid_to",
        *(F.col(c).alias(f"customer_{c}") for c in TRACKED),
    )
    in_range = (
        (F.col("o.order_ts") >= F.col("d.valid_from")) | (F.col("d.version_number") == 1)
    ) & (F.col("d.valid_to").isNull() | (F.col("o.order_ts") < F.col("d.valid_to")))
    return (
        orders.alias("o")
        .join(dim.alias("d"), (F.col("o.customer_id") == F.col("d.customer_id")) & in_range, "left")
        .select(
            "o.order_id",
            "o.customer_id",
            "o.order_ts",
            "d.customer_key",
            "d.customer_full_name",
            "d.customer_email",
            "d.customer_country",
        )
    )


def write_scd2(dim_customer: DataFrame, order_versions: DataFrame, paths: LakePaths) -> None:
    dim_customer.write.mode("overwrite").parquet((paths.gold / "dim_customer").as_posix())
    order_versions.write.mode("overwrite").parquet(
        (paths.gold / "order_customer_version").as_posix()
    )
