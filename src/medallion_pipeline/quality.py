"""Data quality gate that runs after Gold and fails the pipeline when a check breaks."""

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from medallion_pipeline.config import REVENUE_STATUSES
from medallion_pipeline.silver import SilverTables


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str

    def as_dict(self) -> dict:
        return asdict(self)


class DataQualityError(RuntimeError):
    """Raised when at least one quality check fails."""

    def __init__(self, failures: list[CheckResult]) -> None:
        names = ", ".join(failure.name for failure in failures)
        super().__init__(f"{len(failures)} data quality check(s) failed: {names}")
        self.failures = failures


def check_unique(df: DataFrame, table: str, *keys: str) -> CheckResult:
    duplicates = df.groupBy(*keys).count().filter(F.col("count") > 1).count()
    return CheckResult(
        f"unique:{table}", duplicates == 0, f"{duplicates} duplicated key(s) on {list(keys)}"
    )


def check_not_null(df: DataFrame, table: str, column: str) -> CheckResult:
    nulls = df.filter(F.col(column).isNull()).count()
    return CheckResult(f"not_null:{table}.{column}", nulls == 0, f"{nulls} null value(s)")


def check_references(
    child: DataFrame, parent: DataFrame, key: str, relationship: str
) -> CheckResult:
    orphans = child.join(parent.select(key).distinct(), key, "left_anti").count()
    return CheckResult(f"references:{relationship}", orphans == 0, f"{orphans} orphan row(s)")


def _total(df: DataFrame, column: str):
    return df.agg(F.sum(column).alias("total")).first()["total"] or 0


def check_revenue_reconciles(silver: SilverTables, gold: dict[str, DataFrame]) -> CheckResult:
    """Every mart must report the same revenue as the Silver lines it derives from."""
    recognised = silver.orders.filter(F.col("status").isin(*REVENUE_STATUSES)).select("order_id")
    totals = {
        "silver": _total(silver.order_items.join(recognised, "order_id"), "line_total"),
        "daily_revenue": _total(gold["daily_revenue"], "gross_revenue"),
        "customer_lifetime_value": _total(gold["customer_lifetime_value"], "lifetime_revenue"),
        "product_performance": _total(gold["product_performance"], "revenue"),
    }
    return CheckResult("reconciliation:revenue", len(set(totals.values())) == 1, str(totals))


def run_checks(silver: SilverTables, gold: dict[str, DataFrame]) -> list[CheckResult]:
    return [
        check_unique(silver.customers, "silver.customers", "customer_id"),
        check_unique(silver.products, "silver.products", "product_id"),
        check_unique(silver.orders, "silver.orders", "order_id"),
        check_unique(silver.order_items, "silver.order_items", "order_id", "line_number"),
        check_unique(gold["daily_revenue"], "gold.daily_revenue", "order_date"),
        check_unique(
            gold["customer_lifetime_value"], "gold.customer_lifetime_value", "customer_id"
        ),
        check_unique(gold["product_performance"], "gold.product_performance", "product_id"),
        check_not_null(silver.orders, "silver.orders", "order_ts"),
        check_not_null(silver.order_items, "silver.order_items", "line_total"),
        check_references(silver.orders, silver.customers, "customer_id", "orders->customers"),
        check_references(silver.order_items, silver.orders, "order_id", "order_items->orders"),
        check_references(
            silver.order_items, silver.products, "product_id", "order_items->products"
        ),
        check_revenue_reconciles(silver, gold),
    ]


def assert_all_passed(results: list[CheckResult]) -> None:
    failures = [result for result in results if not result.passed]
    if failures:
        raise DataQualityError(failures)
