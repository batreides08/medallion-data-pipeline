import csv
import json
from pathlib import Path

from medallion_pipeline.data_generator import GenerationConfig, generate_raw_data


def _read_all(raw_dir: Path) -> dict[str, bytes]:
    return {path.name: path.read_bytes() for path in raw_dir.iterdir()}


def test_same_seed_produces_identical_files(tmp_path: Path) -> None:
    first = generate_raw_data(tmp_path / "a")
    second = generate_raw_data(tmp_path / "b")

    assert first == second
    assert _read_all(tmp_path / "a") == _read_all(tmp_path / "b")


def test_different_seed_produces_different_orders(tmp_path: Path) -> None:
    generate_raw_data(tmp_path / "a", GenerationConfig(seed=1))
    generate_raw_data(tmp_path / "b", GenerationConfig(seed=2))

    assert _read_all(tmp_path / "a")["orders.jsonl"] != _read_all(tmp_path / "b")["orders.jsonl"]


def test_expected_counts_account_for_every_order(tmp_path: Path) -> None:
    cfg = GenerationConfig()

    expected = generate_raw_data(tmp_path, cfg)

    assert expected.silver_orders + expected.rejected_orders == cfg.n_orders
    assert expected.rejected_orders == (
        cfg.malformed_orders
        + cfg.missing_customer_orders
        + cfg.orphan_customer_orders
        + cfg.invalid_status_orders
        + cfg.invalid_timestamp_orders
    )


def test_order_file_contains_duplicates_and_malformed_lines(tmp_path: Path) -> None:
    cfg = GenerationConfig()
    generate_raw_data(tmp_path, cfg)

    lines = (tmp_path / "orders.jsonl").read_text(encoding="utf-8").splitlines()

    assert len(lines) == cfg.n_orders + cfg.duplicated_orders
    malformed = 0
    for line in lines:
        try:
            json.loads(line)
        except json.JSONDecodeError:
            malformed += 1
    assert malformed == cfg.malformed_orders


def test_customers_file_contains_every_version(tmp_path: Path) -> None:
    cfg = GenerationConfig()
    generate_raw_data(tmp_path, cfg)

    with (tmp_path / "customers.csv").open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == (
        cfg.n_customers + cfg.stale_customer_versions + cfg.invalid_email_customers
    )
