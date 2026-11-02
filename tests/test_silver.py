from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from conftest import bronze_frame
from medallion_pipeline.silver import (
    REJECTION_REASONS,
    apply_rules,
    clean_customers,
    clean_order_items,
    clean_orders,
    clean_products,
)

CUSTOMER_COLUMNS = ["customer_id", "full_name", "email", "country", "updated_at"]
PRODUCT_COLUMNS = ["product_id", "name", "category", "list_price", "updated_at"]
ORDERS_SCHEMA = (
    "order_id string, customer_id string, status string, order_ts string, "
    "items array<struct<product_id:string,quantity:string,unit_price:string>>, "
    "_corrupt_record string"
)
ITEMS = [("P1", "2", "10.50")]


def _customers(spark: SparkSession, *ids: str):
    rows = [(i, "N", f"{i}@example.com", "cr", "2025-01-01T00:00:00Z") for i in ids]
    return clean_customers(bronze_frame(spark, CUSTOMER_COLUMNS, rows)).valid


def _products(spark: SparkSession, *ids: str):
    rows = [(i, "N", "Home", "1.00", "2025-01-01T00:00:00Z") for i in ids]
    return clean_products(bronze_frame(spark, PRODUCT_COLUMNS, rows)).valid


def _orders(spark: SparkSession, rows: list[tuple]):
    return bronze_frame(spark, [], rows, ORDERS_SCHEMA)


def _reasons(split) -> list[str]:
    return [reason for row in split.rejected.collect() for reason in row[REJECTION_REASONS]]


def test_apply_rules_treats_null_as_broken(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (-1,), (None,)], "value int")

    split = apply_rules(df, {"positive": F.col("value") > 0})

    assert [row["value"] for row in split.valid.collect()] == [1]
    assert split.rejected.count() == 2


def test_apply_rules_reports_every_broken_rule(spark: SparkSession) -> None:
    df = spark.createDataFrame([(-1, None)], "a int, b string")

    split = apply_rules(df, {"a_positive": F.col("a") > 0, "b_set": F.col("b").isNotNull()})

    assert split.rejected.first()[REJECTION_REASONS] == ["a_positive", "b_set"]


def test_customers_are_normalised(spark: SparkSession) -> None:
    rows = [(" C1 ", " Ana Soto ", "  ANA@Example.COM ", " cr ", "2025-01-01T00:00:00Z")]

    row = clean_customers(bronze_frame(spark, CUSTOMER_COLUMNS, rows)).valid.first()

    assert row["customer_id"] == "C1"
    assert row["full_name"] == "Ana Soto"
    assert row["email"] == "ana@example.com"
    assert row["country"] == "CR"


def test_customers_keep_newest_version(spark: SparkSession) -> None:
    rows = [
        ("C1", "N", "old@example.com", "CR", "2024-01-01T00:00:00Z"),
        ("C1", "N", "new@example.com", "CR", "2025-01-01T00:00:00Z"),
    ]

    valid = clean_customers(bronze_frame(spark, CUSTOMER_COLUMNS, rows)).valid

    assert [row["email"] for row in valid.collect()] == ["new@example.com"]


def test_customers_with_invalid_email_are_quarantined(spark: SparkSession) -> None:
    rows = [("C1", "N", "not-an-email", "CR", "2025-01-01T00:00:00Z")]

    split = clean_customers(bronze_frame(spark, CUSTOMER_COLUMNS, rows))

    assert split.valid.count() == 0
    assert _reasons(split) == ["email_format"]


def test_products_cast_price_and_reject_bad_values(spark: SparkSession) -> None:
    rows = [
        ("P1", "Lamp", "HOME", "19.99", "2025-01-01T00:00:00Z"),
        ("P2", "Rug", "home", "-1.00", "2025-01-01T00:00:00Z"),
        ("P3", "Vase", "home", "free", "2025-01-01T00:00:00Z"),
    ]

    split = clean_products(bronze_frame(spark, PRODUCT_COLUMNS, rows))

    row = split.valid.first()
    assert split.valid.count() == 1
    assert (row["list_price"], row["category"]) == (Decimal("19.99"), "home")
    assert _reasons(split) == ["list_price_non_negative", "list_price_non_negative"]


def test_valid_order_is_typed_and_keeps_items(spark: SparkSession) -> None:
    bronze = _orders(spark, [("O1", "C1", "Delivered", "2025-03-01T10:00:00Z", ITEMS, None)])

    split = clean_orders(bronze, _customers(spark, "C1"))

    row = split.valid.first()
    assert row["status"] == "delivered"
    assert row["order_ts"].year == 2025
    assert row["items"][0]["product_id"] == "P1"
    assert "_known_customer_id" not in split.valid.columns


def test_order_rules_each_catch_their_defect(spark: SparkSession) -> None:
    ts = "2025-03-01T10:00:00Z"
    cases = {
        "known_customer": ("O1", "C404", "placed", ts, ITEMS, None),
        "known_status": ("O2", "C1", "lost", ts, ITEMS, None),
        "order_ts_parsable": ("O3", "C1", "placed", "not-a-date", ITEMS, None),
        "has_items": ("O4", "C1", "placed", ts, [], None),
    }

    for rule, row in cases.items():
        split = clean_orders(_orders(spark, [row]), _customers(spark, "C1"))
        assert split.valid.count() == 0
        assert _reasons(split) == [rule]


def test_corrupt_record_is_quarantined_with_its_payload(spark: SparkSession) -> None:
    bronze = _orders(spark, [(None, None, None, None, None, '{"order_id": "O1"')])

    split = clean_orders(bronze, _customers(spark, "C1"))

    assert "well_formed_record" in _reasons(split)
    assert split.rejected.first()["_corrupt_record"] == '{"order_id": "O1"'


def test_duplicated_orders_collapse_to_one(spark: SparkSession) -> None:
    row = ("O1", "C1", "placed", "2025-03-01T10:00:00Z", ITEMS, None)

    split = clean_orders(_orders(spark, [row, row]), _customers(spark, "C1"))

    assert split.valid.count() == 1
    assert split.rejected.count() == 0


def _valid_orders(spark: SparkSession, items: list[tuple]):
    bronze = _orders(spark, [("O1", "C1", "placed", "2025-03-01T10:00:00Z", items, None)])
    return clean_orders(bronze, _customers(spark, "C1")).valid


def test_order_items_compute_line_total_and_line_number(spark: SparkSession) -> None:
    items = [("P1", "2", "10.50"), ("P2", "1", "3.25")]

    split = clean_order_items(_valid_orders(spark, items), _products(spark, "P1", "P2"))

    rows = sorted(split.valid.collect(), key=lambda row: row["line_number"])
    assert [row["line_number"] for row in rows] == [1, 2]
    assert [row["line_total"] for row in rows] == [Decimal("21.00"), Decimal("3.25")]


def test_order_items_reject_lines_not_the_order(spark: SparkSession) -> None:
    items = [("P1", "2", "10.50"), ("P1", "0", "10.50"), ("P404", "1", "1.00"), ("P1", "x", "1.00")]

    split = clean_order_items(_valid_orders(spark, items), _products(spark, "P1"))

    assert split.valid.count() == 1
    assert sorted(_reasons(split)) == ["known_product", "quantity_positive", "quantity_positive"]
