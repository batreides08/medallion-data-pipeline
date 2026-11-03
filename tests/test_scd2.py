from datetime import datetime

import pytest
from pyspark.sql import SparkSession

from conftest import bronze_frame
from medallion_pipeline.scd2 import build_dim_customer, orders_with_customer_version

COLUMNS = ["customer_id", "full_name", "email", "country", "updated_at"]


def _customers(spark: SparkSession, rows: list[tuple]):
    return bronze_frame(spark, COLUMNS, rows)


@pytest.fixture
def history(spark: SparkSession):
    return build_dim_customer(
        _customers(
            spark,
            [
                ("C1", "Ana", "ana@x.com", "cr", "2025-01-01T00:00:00Z"),
                ("C1", "Ana", "ana@x.com", "cr", "2025-02-01T00:00:00Z"),  # no change
                ("C1", "Ana", "ana@x.com", "mx", "2025-03-01T00:00:00Z"),  # moved
                ("C2", "Bo", "bo@x.com", "us", "2025-01-15T00:00:00Z"),
                ("C3", "Bad", "not-an-email", "us", "2025-01-15T00:00:00Z"),  # rejected
            ],
        )
    )


def test_only_attribute_changes_create_versions(history) -> None:
    rows = history.filter("customer_id = 'C1'").orderBy("valid_from").collect()

    assert [(r.country, r.version_number, r.is_current) for r in rows] == [
        ("CR", 1, False),
        ("MX", 2, True),
    ]
    assert rows[0].valid_to == rows[1].valid_from == datetime(2025, 3, 1)
    assert rows[1].valid_to is None


def test_invalid_customers_are_not_in_the_dimension(history) -> None:
    assert history.filter("customer_id = 'C3'").count() == 0
    assert history.filter("is_current").count() == 2


def test_orders_join_to_the_version_valid_at_order_time(spark: SparkSession, history) -> None:
    orders = spark.createDataFrame(
        [
            ("O1", "C1", datetime(2025, 2, 10)),
            ("O2", "C1", datetime(2025, 3, 1)),  # boundary belongs to the new version
            ("O3", "C1", datetime(2024, 12, 1)),  # before first version: takes the first
            ("O4", "C2", datetime(2025, 6, 1)),
            ("O5", "C9", datetime(2025, 6, 1)),  # unknown customer: no version
        ],
        "order_id string, customer_id string, order_ts timestamp",
    )

    result = {
        r.order_id: r.customer_country
        for r in orders_with_customer_version(orders, history).collect()
    }

    assert result == {"O1": "CR", "O2": "MX", "O3": "CR", "O4": "US", "O5": None}


def test_same_instant_redelivered_keeps_latest_ingestion(spark: SparkSession) -> None:
    older = bronze_frame(spark, COLUMNS, [("C1", "Ana", "old@x.com", "cr", "2025-01-01T00:00:00Z")])
    newer = spark.createDataFrame(
        [("C1", "Ana", "new@x.com", "cr", "2025-01-01T00:00:00Z", "f", datetime(2026, 5, 5), "b2")],
        [*COLUMNS, "_source_file", "_ingested_at", "_batch_id"],
    )

    rows = build_dim_customer(older.unionByName(newer)).collect()

    assert [r.email for r in rows] == ["new@x.com"]
