"""Incremental Silver loading driven by a watermark of processed Bronze batches.

The watermark is a small JSON file listing the batches already merged into Silver. A run cleans
only the Bronze batches missing from it and merges them into the existing Silver tables, so cost
follows the size of the delivery instead of the size of the history.

Merge rules mirror a full rebuild: the newest ``updated_at`` wins for customers and products (so a
late, older record never overwrites a newer one), the newest ingestion wins for orders, and the
lines of a re-delivered order replace its previous lines. An order whose newest version has no
valid line leaves Silver for quarantine.

Late-arriving dimensions are handled too: an order quarantined earlier because its customer was
unknown, or because one of its lines named an unknown product, is re-evaluated from Bronze once
that customer or product has arrived, as a full rebuild would do.
"""

import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion_pipeline.config import ENTITIES, LakePaths
from medallion_pipeline.silver import (
    REJECTION_REASONS,
    SilverTables,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
    keep_latest,
    split_orders_without_valid_items,
)
from medallion_pipeline.tables import read_table

WATERMARK_FILE = "watermark.json"


def watermark_path(paths: LakePaths) -> Path:
    return paths.state / WATERMARK_FILE


def read_watermark(paths: LakePaths) -> set[str]:
    """Batches already merged into Silver; empty when no incremental state exists."""
    path = watermark_path(paths)
    if not path.exists():
        return set()
    return set(json.loads(path.read_text(encoding="utf-8"))["processed_batches"])


def write_watermark(paths: LakePaths, batches: set[str]) -> None:
    path = watermark_path(paths)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps({"processed_batches": sorted(batches)}, indent=2), encoding="utf-8")
    tmp.replace(path)  # atomic, so a crash never leaves a half-written watermark


def bronze_batches(bronze: dict[str, DataFrame]) -> set[str]:
    batches: set[str] = set()
    for df in bronze.values():
        batches |= {row["_batch_id"] for row in df.select("_batch_id").distinct().collect()}
    return batches


def read_silver(spark: SparkSession, paths: LakePaths) -> SilverTables:
    return SilverTables(
        **{name: read_table(spark, paths.silver / name) for name in ENTITIES},
        quarantine={name: read_table(spark, paths.quarantine / name) for name in ENTITIES},
    )


def late_arriving_orders(
    quarantine: dict[str, DataFrame], customers: DataFrame, products: DataFrame
) -> DataFrame:
    """Order IDs whose quarantine may be stale because a missing dimension row has arrived.

    These are orders quarantined for ``known_customer`` whose customer is now in Silver, and
    orders with a line quarantined for ``known_product`` whose product is now in Silver.
    """
    by_customer = (
        quarantine["orders"]
        .filter(F.array_contains(REJECTION_REASONS, "known_customer"))
        .join(customers.select("customer_id"), "customer_id", "left_semi")
        .select("order_id")
    )
    by_product = (
        quarantine["order_items"]
        .filter(F.array_contains(REJECTION_REASONS, "known_product"))
        .join(products.select("product_id"), "product_id", "left_semi")
        .select("order_id")
    )
    return by_customer.unionByName(by_product).filter(F.col("order_id").isNotNull()).distinct()


def merge_silver(
    existing: SilverTables, bronze: dict[str, DataFrame], new_batches: set[str]
) -> SilverTables:
    """Clean the Bronze rows of ``new_batches`` and merge them into ``existing``."""
    delta = {name: df.filter(F.col("_batch_id").isin(*new_batches)) for name, df in bronze.items()}

    customers_new = clean_customers(delta["customers"])
    products_new = clean_products(delta["products"])
    customers = keep_latest(
        existing.customers.unionByName(customers_new.valid),
        "customer_id",
        F.col("updated_at").desc(),
        F.col("_ingested_at").desc(),
    )
    products = keep_latest(
        existing.products.unionByName(products_new.valid),
        "product_id",
        F.col("updated_at").desc(),
        F.col("_ingested_at").desc(),
    )

    # Every Bronze version of a late-arriving order is cleaned again, so the newest one wins
    # exactly as in a full rebuild. Their earlier Silver rows and quarantine rows are replaced.
    late = late_arriving_orders(existing.quarantine, customers, products)
    late_bronze = (
        bronze["orders"]
        .filter(~F.col("_batch_id").isin(*new_batches))
        .join(late, F.trim(bronze["orders"]["order_id"]) == late["order_id"], "left_semi")
    )

    orders_new = clean_orders(delta["orders"].unionByName(late_bronze), customers)
    items_new = clean_order_items(orders_new.valid, products)
    emptied_new = split_orders_without_valid_items(orders_new.valid, items_new.valid).rejected
    # An emptied order still competes for "newest version" like a full rebuild does: when it
    # wins, the order leaves Silver instead of falling back to an older version.
    orders = keep_latest(
        existing.orders.join(late, "order_id", "left_anti").unionByName(
            orders_new.valid.drop("items")
        ),
        "order_id",
        F.col("_ingested_at").desc(),
    ).join(
        emptied_new.select("order_id", "_batch_id", "_ingested_at"),
        ["order_id", "_batch_id", "_ingested_at"],
        "left_anti",
    )
    redelivered = orders_new.valid.select("order_id").distinct()
    items = (
        existing.order_items.join(redelivered, "order_id", "left_anti")
        .join(late, "order_id", "left_anti")
        .unionByName(items_new.valid)
    )

    rejected_new = {
        "customers": customers_new.rejected,
        "products": products_new.rejected,
        "orders": orders_new.rejected.unionByName(emptied_new, allowMissingColumns=True),
        "order_items": items_new.rejected,
    }
    # Replaying a batch must not duplicate its quarantined rows, and the old quarantine rows of
    # a re-evaluated order are replaced by the outcome of its new evaluation.
    kept = {
        name: existing.quarantine[name].filter(~F.col("_batch_id").isin(*new_batches))
        for name in ENTITIES
    }
    for name in ("orders", "order_items"):
        kept[name] = kept[name].join(late, "order_id", "left_anti")
    quarantine = {name: kept[name].unionByName(rejected_new[name]) for name in ENTITIES}
    return SilverTables(
        customers=customers,
        products=products,
        orders=orders,
        order_items=items,
        quarantine=quarantine,
    )
