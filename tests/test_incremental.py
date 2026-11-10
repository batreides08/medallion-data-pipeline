import json
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline.config import ENTITIES, LakePaths
from medallion_pipeline.data_generator import generate_raw_data
from medallion_pipeline.incremental import read_watermark, write_watermark
from medallion_pipeline.pipeline import run_pipeline
from medallion_pipeline.tables import read_table

pytestmark = pytest.mark.parquet_io


def _rows(spark: SparkSession, path: Path, key: list[str]) -> list[tuple]:
    df = read_table(spark, path).drop("_source_file", "_ingested_at")
    return sorted(tuple(r) for r in df.select(*sorted(df.columns)).orderBy(*key).collect())


def test_watermark_round_trip(tmp_path: Path) -> None:
    paths = LakePaths(tmp_path)
    assert read_watermark(paths) == set()
    write_watermark(paths, {"b2", "b1"})
    assert read_watermark(paths) == {"b1", "b2"}
    assert json.loads((paths.state / "watermark.json").read_text())["processed_batches"] == [
        "b1",
        "b2",
    ]


def test_incremental_matches_full_rebuild(spark: SparkSession, tmp_path: Path) -> None:
    # Two deliveries: the second re-sends all customers and products but only the first 300
    # orders, so repeated records, merges and re-delivered orders are all exercised.
    incr, full = LakePaths(tmp_path / "incr"), LakePaths(tmp_path / "full")
    generate_raw_data(incr.raw)
    run_pipeline(spark, incr, "b1", incremental=True)
    assert read_watermark(incr) == {"b1"}

    orders = (incr.raw / "orders.jsonl").read_text(encoding="utf-8").splitlines()[:300]
    (incr.raw / "orders.jsonl").write_text("\n".join(orders) + "\n", encoding="utf-8")
    run_pipeline(spark, incr, "b2", incremental=True, max_rejection_rate=1.0)
    assert read_watermark(incr) == {"b1", "b2"}

    # Same two batches through a full rebuild.
    shutil.copytree(incr.bronze, full.bronze)
    run_pipeline_full_from_bronze(spark, full)

    keys = {
        "customers": ["customer_id"],
        "products": ["product_id"],
        "orders": ["order_id"],
        "order_items": ["order_id", "line_number"],
    }
    for name in ENTITIES:
        assert _rows(spark, incr.silver / name, keys[name]) == _rows(
            spark, full.silver / name, keys[name]
        ), name


def run_pipeline_full_from_bronze(spark: SparkSession, paths: LakePaths) -> None:
    from medallion_pipeline.bronze import read_bronze
    from medallion_pipeline.silver import build_silver, write_silver

    write_silver(build_silver(read_bronze(spark, paths)), paths)


def test_replaying_a_batch_is_idempotent(spark: SparkSession, tmp_path: Path) -> None:
    paths = LakePaths(tmp_path)
    expected = generate_raw_data(paths.raw)
    run_pipeline(spark, paths, "b1", incremental=True)
    result = run_pipeline(spark, paths, "b1", incremental=True)

    assert result.row_counts["silver.orders"] == expected.silver_orders
    assert result.row_counts["silver.order_items"] == expected.silver_order_items
    assert result.row_counts["quarantine.orders"] == expected.rejected_orders
    assert result.row_counts["quarantine.customers"] == expected.rejected_customers
