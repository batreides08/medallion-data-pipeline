"""Data quality gate that runs after Gold and fails the pipeline when a check breaks."""

from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from medallion_pipeline.config import ENTITIES, REVENUE_STATUSES
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


def check_scd2(
    silver: SilverTables, dim_customer: DataFrame, order_versions: DataFrame
) -> list[CheckResult]:
    """Integrity of the customer history: one open version per customer, no order left unmatched."""
    open_versions = dim_customer.filter(F.col("is_current"))
    current = check_unique(open_versions, "gold.dim_customer(current)", "customer_id")
    missing = (
        silver.customers.select("customer_id")
        .distinct()
        .join(open_versions, "customer_id", "left_anti")
    ).count()
    unmatched = order_versions.filter(F.col("customer_key").isNull()).count()
    return [
        current,
        check_unique(dim_customer, "gold.dim_customer", "customer_id", "valid_from"),
        CheckResult(
            "scd2:every_customer_has_current_version", missing == 0, f"{missing} without one"
        ),
        check_unique(order_versions, "gold.order_customer_version", "order_id"),
        CheckResult(
            "scd2:orders_matched_to_version", unmatched == 0, f"{unmatched} unmatched order(s)"
        ),
        check_references(
            order_versions, silver.orders, "order_id", "order_customer_version->orders"
        ),
    ]


def rejection_rates(row_counts: dict[str, int]) -> dict[str, float]:
    """Share of each entity's rows that went to quarantine, from Silver and quarantine counts.

    An entity with no rows at all has a rate of 0.0 rather than an undefined one.
    """
    rates = {}
    for entity in ENTITIES:
        rejected = row_counts[f"quarantine.{entity}"]
        total = row_counts[f"silver.{entity}"] + rejected
        rates[entity] = rejected / total if total else 0.0
    return rates


def check_rejection_rates(rates: dict[str, float], max_rate: float) -> list[CheckResult]:
    """Fail an entity whose quarantine share is above ``max_rate``.

    A high rate usually means the source changed (a new format, a broken export), not that a few
    records are bad, so the run should stop instead of publishing a thin Silver layer.
    """
    if not 0 <= max_rate <= 1:
        raise ValueError(f"max_rate must be between 0 and 1, got {max_rate}")
    return [
        CheckResult(
            f"rejection_rate:{entity}",
            rate <= max_rate,
            f"{rate:.2%} of rows quarantined (limit {max_rate:.2%})",
        )
        for entity, rate in rates.items()
    ]


def assert_all_passed(results: list[CheckResult]) -> None:
    failures = [result for result in results if not result.passed]
    if failures:
        raise DataQualityError(failures)
