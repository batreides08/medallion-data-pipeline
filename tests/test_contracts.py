import csv
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline.bronze import read_raw
from medallion_pipeline.cli import main
from medallion_pipeline.config import LakePaths
from medallion_pipeline.contracts import (
    RawContractError,
    check_csv_header,
    validate_csv_headers,
)
from medallion_pipeline.data_generator import generate_raw_data
from medallion_pipeline.orchestrator import RetryPolicy
from medallion_pipeline.pipeline import run_pipeline

EXPECTED = ("customer_id", "full_name", "email", "country", "updated_at")


def _write(path: Path, text: str) -> Path:
    path.write_text(text, encoding="utf-8")
    return path


def _rewrite_columns(path: Path, order: list[str], rename: dict[str, str] | None = None) -> None:
    """Rewrite a CSV with its columns in ``order``, optionally renaming some headers."""
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    rename = rename or {}
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([rename.get(name, name) for name in order])
        writer.writerows([row.get(name, "") for name in order] for row in rows)


def test_matching_header_passes(tmp_path: Path) -> None:
    check = check_csv_header(_write(tmp_path / "c.csv", ",".join(EXPECTED) + "\n"), EXPECTED)

    assert check.passed
    assert check.describe() == "c.csv: ok"


def test_reordered_header_passes_because_columns_are_mapped_by_name(tmp_path: Path) -> None:
    header = "email,customer_id,updated_at,country,full_name\n"

    assert check_csv_header(_write(tmp_path / "c.csv", header), EXPECTED).passed


def test_missing_and_extra_columns_are_reported(tmp_path: Path) -> None:
    header = "customer_id,full_name,e_mail,country,updated_at,loyalty_tier\n"

    check = check_csv_header(_write(tmp_path / "c.csv", header), EXPECTED)

    assert not check.passed
    assert check.missing == ("email",)
    assert check.extra == ("e_mail", "loyalty_tier")


def test_extra_columns_alone_do_not_fail(tmp_path: Path) -> None:
    header = ",".join(EXPECTED) + ",loyalty_tier\n"

    check = check_csv_header(_write(tmp_path / "c.csv", header), EXPECTED)

    assert check.passed
    assert check.extra == ("loyalty_tier",)


@pytest.mark.parametrize("second", ["email", "Email"])
def test_a_required_column_given_twice_is_ambiguous(tmp_path: Path, second: str) -> None:
    header = ",".join(EXPECTED) + f",{second}\n"

    check = check_csv_header(_write(tmp_path / "c.csv", header), EXPECTED)

    assert not check.passed
    assert check.duplicated == ("email",)


def test_names_are_matched_exactly(tmp_path: Path) -> None:
    header = "customer_id, full_name,email,Country,updated_at\n"

    check = check_csv_header(_write(tmp_path / "c.csv", header), EXPECTED)

    assert check.missing == ("full_name", "country")


def test_empty_file_is_missing_every_column(tmp_path: Path) -> None:
    check = check_csv_header(_write(tmp_path / "c.csv", ""), EXPECTED)

    assert check.missing == EXPECTED


def test_validation_reports_every_broken_file_at_once(tmp_path: Path) -> None:
    good = _write(tmp_path / "good.csv", ",".join(EXPECTED) + "\n")
    bad_a = _write(tmp_path / "a.csv", "customer_id\n")
    bad_b = _write(tmp_path / "b.csv", "\n")

    with pytest.raises(RawContractError) as error:
        validate_csv_headers({good: EXPECTED, bad_a: EXPECTED, bad_b: EXPECTED})

    assert [failure.file for failure in error.value.failures] == ["a.csv", "b.csv"]
    assert "a.csv: missing full_name, email, country, updated_at" in str(error.value)


def test_reordered_delivery_lands_values_in_the_right_columns(
    spark: SparkSession, tmp_path: Path
) -> None:
    paths = LakePaths(tmp_path)
    generate_raw_data(paths.raw)
    original = read_raw(spark, paths, "b")["customers"]
    reference = original.select(*EXPECTED).orderBy("customer_id").collect()
    _rewrite_columns(
        paths.raw / "customers.csv",
        ["email", "updated_at", "customer_id", "country", "full_name", "loyalty_tier"],
    )

    customers = read_raw(spark, paths, "b")["customers"]

    assert customers.columns[: len(EXPECTED)] == list(EXPECTED)
    assert "loyalty_tier" not in customers.columns
    assert dict(customers.dtypes)["customer_id"] == "string"
    assert customers.select(*EXPECTED).orderBy("customer_id").collect() == reference


def test_renamed_column_stops_the_run_before_bronze_is_written(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = LakePaths(tmp_path)
    generate_raw_data(paths.raw)
    _rewrite_columns(
        paths.raw / "products.csv",
        ["product_id", "name", "category", "list_price", "updated_at"],
        rename={"list_price": "price"},
    )
    calls: list[str] = []

    def counting_read_raw(*args):
        calls.append("read_raw")
        return read_raw(*args)

    monkeypatch.setattr("medallion_pipeline.pipeline.read_raw", counting_read_raw)

    with pytest.raises(RawContractError, match=r"products\.csv: missing list_price; extra price"):
        run_pipeline(spark, paths, "b", retry_policy=RetryPolicy(retries=3))

    assert calls == ["read_raw"]  # a broken contract is not retried
    assert not paths.bronze.exists()


def test_cli_returns_one_when_a_raw_file_breaks_its_contract(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generate_raw_data(tmp_path / "raw")
    _write(tmp_path / "raw" / "customers.csv", "customer_id\n")
    monkeypatch.setattr("medallion_pipeline.cli.create_spark_session", lambda: spark)
    monkeypatch.setattr(spark, "stop", lambda: None)

    assert main(["run", "--data-dir", str(tmp_path)]) == 1
