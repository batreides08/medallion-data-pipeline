import pytest

from medallion_pipeline.config import LakePaths
from medallion_pipeline.history import (
    append_run,
    check_rejection_trend,
    history_path,
    read_history,
)


def _run(rate: float, passed: bool = True) -> dict:
    return {"batch_id": "b", "passed": passed, "batch_rejection_rates": {"orders": rate}}


def test_history_round_trips_and_replaces_a_replayed_batch(tmp_path):
    paths = LakePaths(tmp_path)
    assert read_history(paths) == []
    append_run(paths, "b1", {"silver.orders": 1}, {"orders": 0.01}, True)
    append_run(paths, "b2", {"silver.orders": 2}, {"orders": 0.02}, True)
    append_run(paths, "b1", {"silver.orders": 3}, {"orders": 0.03}, False)
    runs = read_history(paths)
    assert [r["batch_id"] for r in runs] == ["b2", "b1"]
    assert runs[-1]["row_counts"] == {"silver.orders": 3}
    assert runs[-1]["passed"] is False


def test_corrupt_history_lines_are_skipped(tmp_path):
    paths = LakePaths(tmp_path)
    append_run(paths, "b1", {}, {}, True)
    with history_path(paths).open("a", encoding="utf-8") as f:
        f.write("{not json\n")
    assert [r["batch_id"] for r in read_history(paths)] == ["b1"]


def test_trend_is_skipped_without_enough_history():
    (result,) = check_rejection_trend({"orders": 0.9}, [_run(0.01), _run(0.01)])
    assert result.passed and "skipped" in result.detail


def test_trend_flags_a_rate_far_above_the_median():
    history = [_run(0.02), _run(0.03), _run(0.02)]
    ok, bad = (check_rejection_trend({"orders": rate}, history)[0] for rate in (0.06, 0.09))
    assert ok.passed
    assert not bad.passed and bad.name == "rejection_trend:orders"


def test_failed_runs_do_not_move_the_baseline():
    history = [_run(0.02)] * 3 + [_run(0.5, passed=False)] * 5
    (result,) = check_rejection_trend({"orders": 0.5}, history)
    assert not result.passed


def test_negative_margin_is_rejected():
    with pytest.raises(ValueError):
        check_rejection_trend({"orders": 0.0}, [], margin=-0.1)
