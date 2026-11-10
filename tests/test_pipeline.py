import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline import pipeline
from medallion_pipeline.bronze import read_raw
from medallion_pipeline.cli import main
from medallion_pipeline.config import DEFAULT_MAX_REJECTION_RATE, ENTITIES, LakePaths
from medallion_pipeline.data_generator import ExpectedCounts, generate_raw_data
from medallion_pipeline.gold import build_gold
from medallion_pipeline.incremental import read_watermark
from medallion_pipeline.orchestrator import RetryPolicy
from medallion_pipeline.pipeline import batch_row_counts, count_rows, run_pipeline
from medallion_pipeline.quality import (
    CheckResult,
    DataQualityError,
    assert_all_passed,
    run_checks,
)
from medallion_pipeline.silver import SilverTables, build_silver
from medallion_pipeline.tables import is_table, read_table, table_versions


def _assert_matches_generator(counts: dict[str, int], expected: ExpectedCounts) -> None:
    assert counts["silver.customers"] == expected.silver_customers
    assert counts["silver.products"] == expected.silver_products
    assert counts["silver.orders"] == expected.silver_orders
    assert counts["silver.order_items"] == expected.silver_order_items
    assert counts["quarantine.customers"] == expected.rejected_customers
    assert counts["quarantine.products"] == expected.rejected_products
    assert counts["quarantine.orders"] == expected.rejected_orders
    assert counts["quarantine.order_items"] == expected.rejected_order_items


def test_all_layers_in_memory_match_generator_truth(
    spark: SparkSession, lake: LakePaths, expected: ExpectedCounts
) -> None:
    silver = build_silver(read_raw(spark, lake, "batch-1"))
    gold = build_gold(silver)

    _assert_matches_generator(count_rows(silver, gold), expected)
    checks = run_checks(silver, gold)
    assert_all_passed(checks)
    assert len(checks) == 13


def test_cli_run_without_raw_data_fails_clearly(tmp_path: Path) -> None:
    assert main(["run", "--data-dir", str(tmp_path)]) == 2


def test_cli_generate_writes_raw_files(tmp_path: Path) -> None:
    assert main(["generate", "--data-dir", str(tmp_path), "--seed", "7"]) == 0
    assert (tmp_path / "raw" / "orders.jsonl").exists()


@pytest.fixture
def fresh_lake(tmp_path: Path) -> tuple[LakePaths, ExpectedCounts]:
    paths = LakePaths(tmp_path)
    return paths, generate_raw_data(paths.raw)


@pytest.mark.parquet_io
def test_run_persists_every_layer_and_writes_report(
    spark: SparkSession, fresh_lake: tuple[LakePaths, ExpectedCounts]
) -> None:
    paths, expected = fresh_lake

    result = run_pipeline(spark, paths, "batch-1")

    _assert_matches_generator(result.row_counts, expected)
    report = json.loads(result.report_path.read_text(encoding="utf-8"))
    assert report["batch_id"] == "batch-1"
    assert all(check["passed"] for check in report["checks"])
    assert report["max_rejection_rate"] == DEFAULT_MAX_REJECTION_RATE
    total_orders = expected.silver_orders + expected.rejected_orders
    assert report["batch_rejection_rates"]["orders"] == pytest.approx(
        expected.rejected_orders / total_orders
    )
    assert {c["name"] for c in report["checks"]} >= {
        f"rejection_rate:{entity}" for entity in ENTITIES
    }
    names = {c["name"] for c in report["checks"]}
    assert {"scd2:orders_matched_to_version", "unique:gold.dim_customer"} <= names
    assert result.row_counts["gold.dim_customer"] >= expected.silver_customers
    assert result.row_counts["gold.order_customer_version"] == expected.silver_orders


@pytest.mark.parquet_io
def test_rerunning_a_batch_is_idempotent(
    spark: SparkSession, fresh_lake: tuple[LakePaths, ExpectedCounts]
) -> None:
    paths, expected = fresh_lake

    run_pipeline(spark, paths, "batch-1")
    result = run_pipeline(spark, paths, "batch-1")

    _assert_matches_generator(result.row_counts, expected)
    assert (
        spark.read.parquet((paths.bronze / "orders").as_posix())
        .select("_batch_id")
        .distinct()
        .count()
        == 1
    )


@pytest.mark.parquet_io
def test_redelivered_data_does_not_duplicate_silver(
    spark: SparkSession, fresh_lake: tuple[LakePaths, ExpectedCounts]
) -> None:
    paths, expected = fresh_lake

    run_pipeline(spark, paths, "batch-1")
    result = run_pipeline(spark, paths, "batch-2")

    assert result.row_counts["silver.orders"] == expected.silver_orders
    assert result.row_counts["silver.order_items"] == expected.silver_order_items
    assert result.row_counts["silver.customers"] == expected.silver_customers
    # Quarantine accumulates both deliveries, but the rate only measures the latest one.
    assert result.row_counts["quarantine.products"] == 2 * expected.rejected_products
    total_products = expected.silver_products + expected.rejected_products
    assert result.batch_rejection_rates["products"] == pytest.approx(
        expected.rejected_products / total_products
    )


@pytest.mark.parquet_io
def test_failed_quality_gate_still_writes_report(
    spark: SparkSession,
    fresh_lake: tuple[LakePaths, ExpectedCounts],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = fresh_lake
    real_checks = run_checks

    def corrupted_checks(silver, gold):
        results = real_checks(silver, gold)
        return [*results[:-1], type(results[-1])("reconciliation:revenue", False, "forced")]

    monkeypatch.setattr("medallion_pipeline.pipeline.run_checks", corrupted_checks)

    with pytest.raises(DataQualityError):
        run_pipeline(spark, paths, "batch-1")

    report = json.loads((paths.reports / "run_batch-1.json").read_text(encoding="utf-8"))
    checks = {check["name"]: check["passed"] for check in report["checks"]}
    assert checks["reconciliation:revenue"] is False


@pytest.mark.parquet_io
def test_cli_run_returns_zero_and_leaves_the_shared_session_alive(
    spark: SparkSession,
    fresh_lake: tuple[LakePaths, ExpectedCounts],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = fresh_lake
    monkeypatch.setattr("medallion_pipeline.cli.create_spark_session", lambda: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    assert main(["run", "--data-dir", str(paths.root), "--batch-id", "cli"]) == 0
    assert (paths.reports / "run_cli.json").exists()


@pytest.mark.parquet_io
def test_cli_run_returns_one_when_the_quality_gate_fails(
    spark: SparkSession,
    fresh_lake: tuple[LakePaths, ExpectedCounts],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, _ = fresh_lake

    def failing_run(*_args):
        raise DataQualityError([CheckResult("forced", False, "")])

    monkeypatch.setattr("medallion_pipeline.cli.create_spark_session", lambda: spark)
    monkeypatch.setattr("medallion_pipeline.cli.run_pipeline", failing_run)
    monkeypatch.setattr(spark, "stop", lambda: None)

    assert main(["run", "--data-dir", str(paths.root)]) == 1


@pytest.mark.parquet_io
def test_rejection_rate_above_the_limit_fails_the_run_and_is_reported(
    spark: SparkSession, fresh_lake: tuple[LakePaths, ExpectedCounts]
) -> None:
    paths, _ = fresh_lake

    # The generator quarantines about 5.7% of products, so a 5% limit must stop the run.
    with pytest.raises(DataQualityError, match="rejection_rate:products"):
        run_pipeline(spark, paths, "strict", max_rejection_rate=0.05)

    report = json.loads((paths.reports / "run_strict.json").read_text(encoding="utf-8"))
    failed = [c["name"] for c in report["checks"] if not c["passed"]]
    assert failed == ["rejection_rate:products"]
    assert report["max_rejection_rate"] == 0.05


@pytest.mark.parametrize("value", ["1.5", "-0.1", "abc"])
def test_cli_rejects_an_invalid_rejection_rate_limit(tmp_path: Path, value: str) -> None:
    with pytest.raises(SystemExit) as exit_info:
        main(["run", "--data-dir", str(tmp_path), "--max-rejection-rate", value])

    assert exit_info.value.code == 2


def test_batch_row_counts_only_count_the_given_batch(spark: SparkSession) -> None:
    def frame(*batches: str):
        return spark.createDataFrame([(b,) for b in batches], "_batch_id string")

    silver = SilverTables(
        customers=frame("b1", "b2"),
        products=frame("b1"),
        orders=frame("b2", "b2"),
        order_items=frame("b2"),
        quarantine={
            "customers": frame("b1"),
            "products": frame("b1", "b2"),
            "orders": frame("b1"),
            "order_items": frame("b2"),
        },
    )

    assert batch_row_counts(silver, "b2") == {
        "silver.customers": 1,
        "silver.products": 0,
        "silver.orders": 2,
        "silver.order_items": 1,
        "quarantine.customers": 0,
        "quarantine.products": 1,
        "quarantine.orders": 0,
        "quarantine.order_items": 1,
    }


@pytest.mark.parquet_io
def test_transient_failure_in_a_layer_is_retried(
    spark: SparkSession,
    fresh_lake: tuple[LakePaths, ExpectedCounts],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths, expected = fresh_lake
    real_write_gold = pipeline.write_gold
    calls = 0

    def flaky_write_gold(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("transient storage error")
        return real_write_gold(*args, **kwargs)

    monkeypatch.setattr(pipeline, "write_gold", flaky_write_gold)

    result = run_pipeline(spark, paths, "batch-1", retry_policy=RetryPolicy(1, 0))

    assert result.task_attempts == {"bronze": 1, "silver": 1, "gold": 2, "quality": 1, "publish": 1}
    _assert_matches_generator(result.row_counts, expected)


@pytest.mark.parquet_io
def test_failed_quality_gate_is_not_retried(
    spark: SparkSession, fresh_lake: tuple[LakePaths, ExpectedCounts]
) -> None:
    paths, _ = fresh_lake

    with pytest.raises(DataQualityError):
        run_pipeline(
            spark, paths, "batch-1", max_rejection_rate=0.0, retry_policy=RetryPolicy(3, 0)
        )

    history = (paths.state / "run_history.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(history) == 1


@pytest.mark.parquet_io
def test_failed_quality_gate_does_not_publish(
    spark: SparkSession, fresh_lake: tuple[LakePaths, ExpectedCounts]
) -> None:
    paths, expected = fresh_lake
    run_pipeline(spark, paths, "batch-1")
    published = read_table(spark, paths.silver / "orders").count()
    assert published == expected.silver_orders
    versions = table_versions(spark, paths.silver / "orders")

    with pytest.raises(DataQualityError):
        run_pipeline(spark, paths, "batch-2", max_rejection_rate=0.0)

    assert table_versions(spark, paths.silver / "orders") == versions
    assert read_table(spark, paths.silver / "orders").count() == published
    assert read_watermark(paths) == {"batch-1"}


@pytest.mark.parquet_io
def test_earlier_versions_stay_queryable(
    spark: SparkSession, fresh_lake: tuple[LakePaths, ExpectedCounts]
) -> None:
    paths, _ = fresh_lake
    run_pipeline(spark, paths, "batch-1")
    run_pipeline(spark, paths, "batch-1")

    path = paths.gold / "daily_revenue"
    versions = table_versions(spark, path)
    assert len(versions) == 2
    first = read_table(spark, path, version=versions[0])
    assert first.count() == read_table(spark, path).count() > 0
    assert is_table(path)
