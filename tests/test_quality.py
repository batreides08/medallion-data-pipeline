from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline.quality import (
    CheckResult,
    DataQualityError,
    assert_all_passed,
    check_not_null,
    check_references,
    check_rejection_rates,
    check_revenue_reconciles,
    check_unique,
    rejection_rates,
)
from medallion_pipeline.silver import SilverTables


def test_check_unique(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (1,), (2,)], "id int")

    assert not check_unique(df, "t", "id").passed
    assert check_unique(df.distinct(), "t", "id").passed


def test_check_not_null(spark: SparkSession) -> None:
    df = spark.createDataFrame([(1,), (None,)], "id int")

    result = check_not_null(df, "t", "id")

    assert not result.passed
    assert result.detail == "1 null value(s)"


def test_check_references(spark: SparkSession) -> None:
    parent = spark.createDataFrame([(1,)], "id int")
    child = spark.createDataFrame([(1,), (2,)], "id int")

    assert not check_references(child, parent, "id", "child->parent").passed
    assert check_references(parent, child, "id", "parent->child").passed


def _reconciliation(spark: SparkSession, daily_total: str):
    amount = Decimal("10.00")
    orders = spark.createDataFrame([("O1", "placed")], "order_id string, status string")
    items = spark.createDataFrame([("O1", amount)], "order_id string, line_total decimal(14,2)")
    gold = {
        "daily_revenue": spark.createDataFrame(
            [(Decimal(daily_total),)], "gross_revenue decimal(16,2)"
        ),
        "customer_lifetime_value": spark.createDataFrame(
            [(amount,)], "lifetime_revenue decimal(16,2)"
        ),
        "product_performance": spark.createDataFrame([(amount,)], "revenue decimal(16,2)"),
    }
    silver = SilverTables(orders, orders, orders, items, quarantine={})
    return check_revenue_reconciles(silver, gold)


def test_reconciliation_passes_when_all_totals_match(spark: SparkSession) -> None:
    assert _reconciliation(spark, "10.00").passed


def test_reconciliation_detects_a_mart_that_drifted(spark: SparkSession) -> None:
    assert not _reconciliation(spark, "10.01").passed


def test_assert_all_passed_names_the_failures() -> None:
    results = [CheckResult("good", True, ""), CheckResult("bad", False, "")]

    with pytest.raises(DataQualityError, match=r"1 data quality check.*bad") as error:
        assert_all_passed(results)

    assert [failure.name for failure in error.value.failures] == ["bad"]


def test_assert_all_passed_accepts_clean_results() -> None:
    assert_all_passed([CheckResult("good", True, "")])


def _counts(**rejected: int) -> dict[str, int]:
    """Row counts with 90 valid rows per entity and the given number of rejected ones."""
    counts = {}
    for entity in ("customers", "products", "orders", "order_items"):
        counts[f"silver.{entity}"] = 90
        counts[f"quarantine.{entity}"] = rejected.get(entity, 0)
    return counts


def test_rejection_rates_are_the_quarantined_share_of_each_entity() -> None:
    rates = rejection_rates(_counts(customers=10, orders=30))

    assert rates == {"customers": 0.1, "products": 0.0, "orders": 0.25, "order_items": 0.0}


def test_rejection_rate_of_an_empty_entity_is_zero() -> None:
    counts = _counts()
    counts["silver.products"] = 0

    assert rejection_rates(counts)["products"] == 0.0


def test_rejection_rate_check_fails_only_the_entity_above_the_limit() -> None:
    results = check_rejection_rates({"customers": 0.10, "orders": 0.25}, max_rate=0.10)

    assert [(r.name, r.passed) for r in results] == [
        ("rejection_rate:customers", True),
        ("rejection_rate:orders", False),
    ]
    assert results[1].detail == "25.00% of rows quarantined (limit 10.00%)"


@pytest.mark.parametrize("max_rate", [-0.01, 1.01])
def test_rejection_rate_limit_must_be_a_fraction(max_rate: float) -> None:
    with pytest.raises(ValueError, match="between 0 and 1"):
        check_rejection_rates({"orders": 0.0}, max_rate)
