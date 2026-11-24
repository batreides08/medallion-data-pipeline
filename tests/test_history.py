import pytest

from medallion_pipeline.cli import main
from medallion_pipeline.config import LakePaths
from medallion_pipeline.history import (
    append_run,
    check_rejection_trend,
    find_slow_tasks,
    format_history,
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


def _timed(seconds: dict[str, float], passed: bool = True) -> dict:
    return {"batch_id": "b", "passed": passed, "task_seconds": seconds}


def test_task_seconds_are_recorded_and_default_to_empty(tmp_path):
    paths = LakePaths(tmp_path)
    append_run(paths, "b1", {}, {}, True, {"silver": 12.5})
    append_run(paths, "b2", {}, {}, True)
    first, second = read_history(paths)
    assert first["task_seconds"] == {"silver": 12.5}
    assert second["task_seconds"] == {}


def test_a_task_far_slower_than_its_median_is_flagged():
    history = [_timed({"silver": 10.0, "gold": 4.0}) for _ in range(3)]
    (slow,) = find_slow_tasks({"silver": 25.0, "gold": 7.0}, history)
    assert slow.name == "silver"
    assert slow.median_seconds == 10.0 and slow.baseline_runs == 3
    assert slow.as_dict() == {
        "name": "silver",
        "seconds": 25.0,
        "median_seconds": 10.0,
        "baseline_runs": 3,
    }


def test_a_short_task_that_doubles_from_noise_is_not_flagged():
    history = [_timed({"publish": 0.5})] * 3
    assert find_slow_tasks({"publish": 3.0}, history) == []
    assert [s.name for s in find_slow_tasks({"publish": 3.0}, history, min_seconds=1)] == [
        "publish"
    ]


def test_slowdown_needs_enough_passing_runs_with_timings():
    old_runs = [{"batch_id": "b", "passed": True}] * 5  # recorded before timings were kept
    failed = [_timed({"silver": 1.0}, passed=False)] * 5
    history = [*old_runs, *failed, _timed({"silver": 1.0}), _timed({"silver": 1.0})]
    assert find_slow_tasks({"silver": 100.0}, history) == []


@pytest.mark.parametrize(("factor", "min_seconds"), [(0.5, 5.0), (2.0, -1.0)])
def test_invalid_slowdown_settings_are_rejected(factor, min_seconds):
    with pytest.raises(ValueError):
        find_slow_tasks({}, [], factor=factor, min_seconds=min_seconds)


def test_history_table_shows_recent_runs_newest_last():
    runs = [
        {"batch_id": "b1", "recorded_at": "t1", "passed": True},
        {
            "batch_id": "b2",
            "recorded_at": "t2",
            "passed": False,
            "batch_rejection_rates": {"orders": 0.02, "products": 0.25},
            "task_seconds": {"bronze": 3.0, "silver": 9.25},
        },
    ]
    lines = format_history(runs).splitlines()
    assert lines[0].split() == [
        "batch_id",
        "recorded_at",
        "gate",
        "total_s",
        "max_rejection",
        "slowest_task",
    ]
    assert lines[1].split() == ["b1", "t1", "passed", "-", "-", "-"]
    assert lines[2].split() == [
        *("b2", "t2", "failed", "12.2"),
        *("products", "25.00%", "silver", "9.2s"),
    ]
    assert len(format_history(runs, last=1).splitlines()) == 2


def test_cli_history_prints_runs_or_fails_without_history(tmp_path, capsys):
    assert main(["history", "--data-dir", str(tmp_path)]) == 2
    paths = LakePaths(tmp_path)
    for batch in ("b1", "b2", "b3"):
        append_run(paths, batch, {}, {"orders": 0.01}, True, {"silver": 2.0})
    assert main(["history", "--data-dir", str(tmp_path), "--last", "2"]) == 0
    out = capsys.readouterr().out.splitlines()
    assert [line.split()[0] for line in out] == ["batch_id", "b2", "b3"]


def test_cli_history_rejects_zero_runs(tmp_path):
    with pytest.raises(SystemExit):
        main(["history", "--data-dir", str(tmp_path), "--last", "0"])
