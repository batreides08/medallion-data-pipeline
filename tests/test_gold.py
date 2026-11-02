from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline.gold import build_gold, load_query
from medallion_pipeline.silver import SilverTables


@pytest.fixture
def silver(spark: SparkSession) -> SilverTables:
    customers = spark.createDataFrame(
        [("C1", "CR"), ("C2", "MX"), ("C3", "US")], "customer_id string, country string"
    )
    products = spark.createDataFrame(
        [("P1", "Lamp", "home"), ("P2", "Rug", "home"), ("P3", "Ball", "sports")],
        "product_id string, name string, category string",
    )
    orders = spark.createDataFrame(
        [
            ("O1", "C1", "delivered", datetime(2025, 3, 1, 10, tzinfo=UTC)),
            ("O2", "C1", "shipped", datetime(2025, 3, 1, 23, tzinfo=UTC)),
            ("O3", "C2", "placed", datetime(2025, 3, 2, 8, tzinfo=UTC)),
            ("O4", "C2", "cancelled", datetime(2025, 3, 2, 9, tzinfo=UTC)),
            ("O5", "C3", "returned", datetime(2025, 3, 3, 9, tzinfo=UTC)),
        ],
        "order_id string, customer_id string, status string, order_ts timestamp",
    )
    items = spark.createDataFrame(
        [
            ("O1", 1, "P1", 2, Decimal("100.00")),
            ("O1", 2, "P3", 1, Decimal("50.00")),
            ("O2", 1, "P1", 1, Decimal("50.00")),
            ("O3", 1, "P2", 1, Decimal("1000.00")),
            ("O4", 1, "P2", 9, Decimal("9000.00")),
            ("O5", 1, "P3", 9, Decimal("9000.00")),
        ],
        "order_id string, line_number int, product_id string, quantity int, "
        "line_total decimal(14,2)",
    )
    return SilverTables(customers, products, orders, items, quarantine={})


def test_load_query_fills_in_revenue_statuses() -> None:
    query = load_query("daily_revenue")

    assert "'delivered'" in query
    assert "{{" not in query


def test_daily_revenue_excludes_cancelled_and_returned(silver: SilverTables) -> None:
    rows = build_gold(silver)["daily_revenue"].collect()

    assert [tuple(row) for row in rows] == [
        (date(2025, 3, 1), 2, 4, Decimal("200.00"), Decimal("100.00")),
        (date(2025, 3, 2), 1, 1, Decimal("1000.00"), Decimal("1000.00")),
    ]


def test_customer_lifetime_value_segments_every_customer(silver: SilverTables) -> None:
    rows = {r["customer_id"]: r for r in build_gold(silver)["customer_lifetime_value"].collect()}

    assert rows["C1"]["orders"] == 2
    assert rows["C1"]["lifetime_revenue"] == Decimal("200.00")
    assert rows["C1"]["value_segment"] == "occasional"
    assert rows["C2"]["value_segment"] == "regular"
    assert rows["C3"]["orders"] == 0
    assert rows["C3"]["first_order_ts"] is None
    assert rows["C3"]["value_segment"] == "inactive"


def test_product_performance_ranks_inside_category(silver: SilverTables) -> None:
    rows = {r["product_id"]: r for r in build_gold(silver)["product_performance"].collect()}

    assert rows["P2"]["revenue"] == Decimal("1000.00")
    assert rows["P2"]["category_revenue_rank"] == 1
    assert rows["P1"]["units_sold"] == 3
    assert rows["P1"]["category_revenue_rank"] == 2
    assert rows["P3"]["category_revenue_rank"] == 1
