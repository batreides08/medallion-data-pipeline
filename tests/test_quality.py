from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline.quality import (
    CheckResult,
    DataQualityError,
    assert_all_passed,
    check_not_null,
    check_references,
    check_revenue_reconciles,
    check_unique,
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
