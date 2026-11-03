"""Silver layer: typed, validated and deduplicated entities.

A row that breaks a rule is never dropped silently: it moves to a quarantine table together with
the names of the rules it broke. All functions are pure DataFrame transformations.
"""

from collections.abc import Mapping
from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from medallion_pipeline.bronze import CORRUPT_RECORD
from medallion_pipeline.config import ORDER_STATUSES, LakePaths

REJECTION_REASONS = "_rejection_reasons"
EMAIL_PATTERN = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
LINEAGE = ("_source_file", "_ingested_at", "_batch_id")


@dataclass(frozen=True)
class Split:
    """Rows that passed every rule, and rows that broke at least one."""

    valid: DataFrame
    rejected: DataFrame


@dataclass(frozen=True)
class SilverTables:
    customers: DataFrame
    products: DataFrame
    orders: DataFrame
    order_items: DataFrame
    quarantine: dict[str, DataFrame]

    def as_dict(self) -> dict[str, DataFrame]:
        return {
            "customers": self.customers,
            "products": self.products,
            "orders": self.orders,
            "order_items": self.order_items,
        }


def apply_rules(df: DataFrame, rules: Mapping[str, Column]) -> Split:
    """Split ``df`` by ``rules``, a mapping of rule name to boolean condition.

    A condition evaluating to NULL counts as broken, so missing values cannot slip through.
    """
    broken = [
        F.when(~F.coalesce(condition, F.lit(False)), F.lit(name))
        for name, condition in rules.items()
    ]
    flagged = df.withColumn(REJECTION_REASONS, F.filter(F.array(*broken), lambda r: r.isNotNull()))
    is_valid = F.size(REJECTION_REASONS) == 0
    return Split(flagged.filter(is_valid).drop(REJECTION_REASONS), flagged.filter(~is_valid))


def keep_latest(df: DataFrame, key: str, *newest_first: Column) -> DataFrame:
    """Keep one row per ``key``: the first one according to ``newest_first``."""
    window = Window.partitionBy(key).orderBy(*newest_first)
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def validate_customers(bronze: DataFrame) -> Split:
    """Type and validate customer rows, keeping every version (no deduplication)."""
    typed = bronze.select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("full_name").alias("full_name"),
        F.lower(F.trim("email")).alias("email"),
        F.upper(F.trim("country")).alias("country"),
        F.try_to_timestamp(F.col("updated_at")).alias("updated_at"),
        *LINEAGE,
    )
    split = apply_rules(
        typed,
        {
            "customer_id_present": F.col("customer_id").isNotNull(),
            "email_format": F.col("email").rlike(EMAIL_PATTERN),
            "updated_at_parsable": F.col("updated_at").isNotNull(),
        },
    )
    return split


def clean_customers(bronze: DataFrame) -> Split:
    split = validate_customers(bronze)
    latest = keep_latest(
        split.valid, "customer_id", F.col("updated_at").desc(), F.col("_ingested_at").desc()
    )
    return Split(latest, split.rejected)


def clean_products(bronze: DataFrame) -> Split:
    typed = bronze.select(
        F.trim("product_id").alias("product_id"),
        F.trim("name").alias("name"),
        F.lower(F.trim("category")).alias("category"),
        F.col("list_price").try_cast("decimal(12,2)").alias("list_price"),
        F.try_to_timestamp(F.col("updated_at")).alias("updated_at"),
        *LINEAGE,
    )
    split = apply_rules(
        typed,
        {
            "product_id_present": F.col("product_id").isNotNull(),
            "list_price_non_negative": F.col("list_price") >= 0,
            "updated_at_parsable": F.col("updated_at").isNotNull(),
        },
    )
    latest = keep_latest(
        split.valid, "product_id", F.col("updated_at").desc(), F.col("_ingested_at").desc()
    )
    return Split(latest, split.rejected)


def clean_orders(bronze: DataFrame, customers: DataFrame) -> Split:
    """Validate order headers; ``valid`` keeps the nested ``items`` for the next step."""
    known = customers.select(F.col("customer_id").alias("_known_customer_id"))
    typed = bronze.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.lower(F.trim("status")).alias("status"),
        F.try_to_timestamp(F.col("order_ts")).alias("order_ts"),
        "items",
        CORRUPT_RECORD,
        *LINEAGE,
    ).join(known, F.col("customer_id") == F.col("_known_customer_id"), "left")
    split = apply_rules(
        typed,
        {
            "well_formed_record": F.col(CORRUPT_RECORD).isNull(),
            "order_id_present": F.col("order_id").isNotNull(),
            "known_customer": F.col("_known_customer_id").isNotNull(),
            "known_status": F.col("status").isin(*ORDER_STATUSES),
            "order_ts_parsable": F.col("order_ts").isNotNull(),
            "has_items": F.size("items") > 0,
        },
    )
    latest = keep_latest(split.valid, "order_id", F.col("_ingested_at").desc())
    return Split(
        latest.drop("_known_customer_id", CORRUPT_RECORD),
        split.rejected.drop("_known_customer_id"),
    )


def clean_order_items(orders: DataFrame, products: DataFrame) -> Split:
    """Explode valid orders into one row per line and validate each line."""
    known = products.select(F.col("product_id").alias("_known_product_id"))
    exploded = orders.select(
        "order_id", F.posexplode("items").alias("line_number", "item"), *LINEAGE
    )
    typed = exploded.select(
        "order_id",
        (F.col("line_number") + 1).alias("line_number"),
        F.trim("item.product_id").alias("product_id"),
        F.col("item.quantity").try_cast("int").alias("quantity"),
        F.col("item.unit_price").try_cast("decimal(12,2)").alias("unit_price"),
        *LINEAGE,
    ).join(known, F.col("product_id") == F.col("_known_product_id"), "left")
    split = apply_rules(
        typed,
        {
            "known_product": F.col("_known_product_id").isNotNull(),
            "quantity_positive": F.col("quantity") > 0,
            "unit_price_non_negative": F.col("unit_price") >= 0,
        },
    )
    valid = split.valid.drop("_known_product_id").withColumn(
        "line_total", (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)")
    )
    return Split(valid, split.rejected.drop("_known_product_id"))


def build_silver(bronze: dict[str, DataFrame]) -> SilverTables:
    """Turn the three Bronze tables into the four Silver tables."""
    customers = clean_customers(bronze["customers"])
    products = clean_products(bronze["products"])
    orders = clean_orders(bronze["orders"], customers.valid)
    items = clean_order_items(orders.valid, products.valid)
    return SilverTables(
        customers=customers.valid,
        products=products.valid,
        orders=orders.valid.drop("items"),
        order_items=items.valid,
        quarantine={
            "customers": customers.rejected,
            "products": products.rejected,
            "orders": orders.rejected,
            "order_items": items.rejected,
        },
    )


def write_silver(silver: SilverTables, paths: LakePaths) -> None:
    for name, df in silver.as_dict().items():
        df.write.mode("overwrite").parquet((paths.silver / name).as_posix())
    for name, df in silver.quarantine.items():
        df.write.mode("overwrite").parquet((paths.quarantine / name).as_posix())
