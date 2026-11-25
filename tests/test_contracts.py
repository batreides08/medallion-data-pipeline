import csv
import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline.bronze import read_raw
from medallion_pipeline.cli import main
from medallion_pipeline.config import LakePaths
from medallion_pipeline.contracts import (
    RawContractError,
    check_csv_header,
    check_jsonl_keys,
    validate_csv_headers,
)
from medallion_pipeline.data_generator import generate_raw_data
from medallion_pipeline.orchestrator import RetryPolicy
from medallion_pipeline.pipeline import run_pipeline

EXPECTED = ("customer_id", "full_name", "email", "country", "updated_at")
ORDER_KEYS = ("order_id", "status", "items")
ITEM_KEYS = ("product_id", "quantity")
ORDER = {"order_id": "O1", "status": "placed", "items": [{"product_id": "P1", "quantity": 1}]}


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


def _write_jsonl(path: Path, *lines: object) -> Path:
    """Write one line per value; strings are written verbatim, anything else as JSON."""
    text = "\n".join(line if isinstance(line, str) else json.dumps(line) for line in lines)
    return _write(path, text + "\n")


def _rename_order_keys(path: Path, rename: dict[str, str]) -> None:
    """Rename top-level keys in every well-formed order of a JSONL file, keeping other lines."""
    lines = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            order = json.loads(line)
        except ValueError:
            lines.append(line)
            continue
        lines.append(json.dumps({rename.get(key, key): value for key, value in order.items()}))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _check_orders(path: Path):
    return check_jsonl_keys(path, ORDER_KEYS, {"items": ITEM_KEYS})


def test_jsonl_with_every_key_passes(tmp_path: Path) -> None:
    check = _check_orders(_write_jsonl(tmp_path / "o.jsonl", ORDER))

    assert check.passed
    assert check.describe() == "o.jsonl: ok"


def test_key_missing_from_some_records_is_a_row_defect_not_a_contract_break(
    tmp_path: Path,
) -> None:
    partial = {"order_id": "O2", "items": [{"product_id": "P2"}]}

    assert _check_orders(_write_jsonl(tmp_path / "o.jsonl", partial, ORDER)).passed


def test_renamed_top_level_key_is_missing_and_extra(tmp_path: Path) -> None:
    renamed = {"order_id": "O1", "order_status": "placed", "items": ORDER["items"]}

    check = _check_orders(_write_jsonl(tmp_path / "o.jsonl", renamed, renamed))

    assert not check.passed
    assert check.missing == ("status",)
    assert check.extra == ("order_status",)
    assert check.describe() == "o.jsonl: missing status; extra order_status"


def test_nested_keys_are_checked_inside_the_list(tmp_path: Path) -> None:
    order = {**ORDER, "items": [{"product_id": "P1", "qty": 1, "discount": 0}]}

    check = _check_orders(_write_jsonl(tmp_path / "o.jsonl", order))

    assert check.missing == ("items.quantity",)
    assert check.extra == ("items.discount", "items.qty")


def test_missing_list_key_is_reported_once_without_its_children(tmp_path: Path) -> None:
    check = _check_orders(_write_jsonl(tmp_path / "o.jsonl", {"order_id": "O1", "status": "x"}))

    assert check.missing == ("items",)


def test_jsonl_keys_are_matched_exactly(tmp_path: Path) -> None:
    order = {"Order_ID": "O1", "status": "placed", "items": ORDER["items"]}

    check = _check_orders(_write_jsonl(tmp_path / "o.jsonl", order))

    assert check.missing == ("order_id",)
    assert check.extra == ("Order_ID",)


def test_malformed_and_non_object_lines_are_left_to_quarantine(tmp_path: Path) -> None:
    path = _write_jsonl(tmp_path / "o.jsonl", '{"order_id": "O9", "sta', "[1, 2]", "", ORDER)

    assert _check_orders(path).passed


@pytest.mark.parametrize("lines", [(), ("not json",)])
def test_jsonl_without_any_object_has_nothing_to_compare(
    tmp_path: Path, lines: tuple[str, ...]
) -> None:
    path = _write(tmp_path / "o.jsonl", "\n".join(lines))

    assert _check_orders(path).passed


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


def test_renamed_order_key_stops_the_run_before_bronze_is_written(
    spark: SparkSession, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    paths = LakePaths(tmp_path)
    generate_raw_data(paths.raw)
    _rename_order_keys(paths.raw / "orders.jsonl", {"order_ts": "orderTs"})
    calls: list[str] = []

    def counting_read_raw(*args):
        calls.append("read_raw")
        return read_raw(*args)

    monkeypatch.setattr("medallion_pipeline.pipeline.read_raw", counting_read_raw)

    with pytest.raises(RawContractError, match=r"orders\.jsonl: missing order_ts; extra orderTs"):
        run_pipeline(spark, paths, "b", retry_policy=RetryPolicy(retries=3))

    assert calls == ["read_raw"]  # a broken contract is not retried
    assert not paths.bronze.exists()


def test_broken_csv_and_jsonl_are_reported_together(spark: SparkSession, tmp_path: Path) -> None:
    paths = LakePaths(tmp_path)
    generate_raw_data(paths.raw)
    _write(paths.raw / "customers.csv", "customer_id\n")
    _rename_order_keys(paths.raw / "orders.jsonl", {"items": "lines"})

    with pytest.raises(RawContractError) as error:
        read_raw(spark, paths, "b")

    assert [failure.file for failure in error.value.failures] == ["customers.csv", "orders.jsonl"]
    assert "orders.jsonl: missing items; extra lines" in str(error.value)


def test_generated_orders_satisfy_the_contract_and_extra_keys_are_ignored(
    spark: SparkSession, tmp_path: Path
) -> None:
    paths = LakePaths(tmp_path)
    generate_raw_data(paths.raw)
    reference = read_raw(spark, paths, "b")["orders"].count()
    _rename_order_keys(paths.raw / "orders.jsonl", {})  # normalise, then add an extra key
    text = (paths.raw / "orders.jsonl").read_text(encoding="utf-8")
    (paths.raw / "orders.jsonl").write_text(
        text.replace('{"order_id"', '{"channel": "web", "order_id"'), encoding="utf-8"
    )

    orders = read_raw(spark, paths, "b")["orders"]

    assert "channel" not in orders.columns
    assert orders.count() == reference
