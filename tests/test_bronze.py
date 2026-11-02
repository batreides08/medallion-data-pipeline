from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from medallion_pipeline.bronze import CORRUPT_RECORD, read_raw
from medallion_pipeline.config import LakePaths
from medallion_pipeline.data_generator import ExpectedCounts, GenerationConfig


def test_read_raw_keeps_every_delivered_row(
    spark: SparkSession, lake: LakePaths, expected: ExpectedCounts
) -> None:
    cfg = GenerationConfig()

    bronze = read_raw(spark, lake, "batch-1")

    assert bronze["orders"].count() == cfg.n_orders + cfg.duplicated_orders
    assert bronze["products"].count() == cfg.n_products + cfg.invalid_price_products


def test_malformed_json_is_captured_rather_than_lost(
    spark: SparkSession, lake: LakePaths, expected: ExpectedCounts
) -> None:
    orders = read_raw(spark, lake, "batch-1")["orders"].cache()

    assert orders.filter(F.col(CORRUPT_RECORD).isNotNull()).count() == (
        GenerationConfig().malformed_orders
    )


def test_every_table_gets_lineage_columns(
    spark: SparkSession, lake: LakePaths, expected: ExpectedCounts
) -> None:
    for df in read_raw(spark, lake, "batch-7").values():
        row = df.select("_source_file", "_ingested_at", "_batch_id").first()
        assert row["_batch_id"] == "batch-7"
        assert row["_source_file"]
        assert row["_ingested_at"]


def test_csv_fields_stay_strings_so_bad_values_cannot_fail_ingestion(
    spark: SparkSession, lake: LakePaths, expected: ExpectedCounts
) -> None:
    products = read_raw(spark, lake, "batch-1")["products"]

    assert dict(products.dtypes)["list_price"] == "string"
