from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from medallion_pipeline.cli import main
from medallion_pipeline.config import LakePaths
from medallion_pipeline.dashboard import build_dashboard, render_dashboard
from medallion_pipeline.pipeline import run_pipeline

DAILY = [
    {"order_date": "2026-01-01", "orders": 2, "units_sold": 5, "gross_revenue": Decimal("100.00")},
    {"order_date": "2026-01-02", "orders": 1, "units_sold": 1, "gross_revenue": Decimal("50.00")},
]
CUSTOMERS = [
    {"country": "CR", "lifetime_revenue": Decimal("120.00"), "value_segment": "regular"},
    {"country": "US", "lifetime_revenue": Decimal("30.00"), "value_segment": "occasional"},
    {"country": "US", "lifetime_revenue": Decimal("0"), "value_segment": "inactive"},
]
PRODUCTS = [
    {"name": "<b>Lamp</b>", "category": "home", "revenue": Decimal("90.00")},
    {"name": "Desk", "category": "home", "revenue": Decimal("60.00")},
]


def test_render_shows_totals_and_escapes_html() -> None:
    page = render_dashboard(DAILY, CUSTOMERS, PRODUCTS, batch_label="b1")
    assert "$150.00" in page  # gross revenue
    assert "$50.00" in page  # average order value: 150 / 3
    assert "2 customers" not in page and "1 customers" in page
    assert "&lt;b&gt;Lamp&lt;/b&gt;" in page and "<b>Lamp</b>" not in page
    assert "batch b1" in page
    assert "<script" not in page


def test_render_handles_empty_marts() -> None:
    page = render_dashboard([], [], [])
    assert "$0.00" in page and "No revenue yet." in page


def test_cli_dashboard_without_gold_fails_clearly(tmp_path: Path) -> None:
    assert main(["dashboard", "--data-dir", str(tmp_path)]) == 2


@pytest.mark.parquet_io
def test_build_dashboard_from_the_lake(spark: SparkSession, tmp_path: Path) -> None:
    from medallion_pipeline.data_generator import generate_raw_data

    paths = LakePaths(tmp_path)
    generate_raw_data(paths.raw)
    run_pipeline(spark, paths, "dash-1", max_rejection_rate=1.0)
    out = build_dashboard(spark, paths)
    page = out.read_text(encoding="utf-8")
    assert out == tmp_path / "dashboard" / "index.html"
    assert "Gross revenue" in page and "<polyline" in page
