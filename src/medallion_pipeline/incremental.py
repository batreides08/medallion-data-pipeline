"""Incremental Silver loading driven by a watermark of processed Bronze batches.

The watermark is a small JSON file listing the batches already merged into Silver. A run cleans
only the Bronze batches missing from it and merges them into the existing Silver tables, so cost
follows the size of the delivery instead of the size of the history.

Merge rules mirror a full rebuild: the newest ``updated_at`` wins for customers and products (so a
late, older record never overwrites a newer one), the newest ingestion wins for orders, and the
lines of a re-delivered order replace its previous lines.
"""

import json
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from medallion_pipeline.config import ENTITIES, LakePaths
from medallion_pipeline.silver import (
    SilverTables,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
    keep_latest,
)

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
        **{name: spark.read.parquet((paths.silver / name).as_posix()) for name in ENTITIES},
        quarantine={
            name: spark.read.parquet((paths.quarantine / name).as_posix()) for name in ENTITIES
        },
    )


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

    orders_new = clean_orders(delta["orders"], customers)
    items_new = clean_order_items(orders_new.valid, products)
    orders = keep_latest(
        existing.orders.unionByName(orders_new.valid.drop("items")),
        "order_id",
        F.col("_ingested_at").desc(),
    )
    redelivered = orders_new.valid.select("order_id").distinct()
    items = existing.order_items.join(redelivered, "order_id", "left_anti").unionByName(
        items_new.valid
    )

    rejected_new = {
        "customers": customers_new.rejected,
        "products": products_new.rejected,
        "orders": orders_new.rejected,
        "order_items": items_new.rejected,
    }
    # Replaying a batch must not duplicate its quarantined rows.
    quarantine = {
        name: existing.quarantine[name]
        .filter(~F.col("_batch_id").isin(*new_batches))
        .unionByName(rejected_new[name])
        for name in ENTITIES
    }
    return SilverTables(
        customers=customers,
        products=products,
        orders=orders,
        order_items=items,
        quarantine=quarantine,
    )


def write_silver_merged(silver: SilverTables, paths: LakePaths) -> None:
    """Write merged tables via a staging directory.

    The merged frames read from the directories they replace, and Spark cannot overwrite a path it
    is reading, so every table is materialised in staging before the live copy is swapped.
    """
    staging = paths.state / "staging"
    shutil.rmtree(staging, ignore_errors=True)
    targets = [
        *((df, paths.silver / name) for name, df in silver.as_dict().items()),
        *((df, paths.quarantine / name) for name, df in silver.quarantine.items()),
    ]
    staged: list[tuple[Path, Path]] = []
    for df, live in targets:
        stage = staging / live.parent.name / live.name
        df.write.mode("overwrite").parquet(stage.as_posix())
        staged.append((stage, live))
    for stage, live in staged:
        shutil.rmtree(live, ignore_errors=True)
        shutil.move(stage, live)
    shutil.rmtree(staging, ignore_errors=True)
