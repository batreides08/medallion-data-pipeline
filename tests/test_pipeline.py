import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline.bronze import read_raw
from medallion_pipeline.cli import main
from medallion_pipeline.config import LakePaths
from medallion_pipeline.data_generator import ExpectedCounts, generate_raw_data
from medallion_pipeline.gold import build_gold
from medallion_pipeline.pipeline import count_rows, run_pipeline
from medallion_pipeline.quality import (
    CheckResult,
    DataQualityError,
    assert_all_passed,
    run_checks,
)
from medallion_pipeline.silver import build_silver


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
    assert report["checks"][-1]["passed"] is False


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
