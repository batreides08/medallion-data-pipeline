import pytest

from medallion_pipeline.cli import main
from medallion_pipeline.orchestrator import RetryPolicy, Task, TaskRun, run_tasks


class Flaky:
    """Fails a fixed number of times, then succeeds."""

    def __init__(self, failures: int) -> None:
        self.failures = failures
        self.calls = 0

    def __call__(self) -> None:
        self.calls += 1
        if self.calls <= self.failures:
            raise OSError("transient")


def test_tasks_run_in_order_with_one_attempt_each() -> None:
    order: list[str] = []
    tasks = [Task(name, lambda name=name: order.append(name)) for name in ("a", "b", "c")]

    assert run_tasks(tasks) == {"a": 1, "b": 1, "c": 1}
    assert order == ["a", "b", "c"]


def test_failing_task_is_retried_with_doubling_backoff() -> None:
    flaky = Flaky(failures=2)
    waits: list[float] = []

    attempts = run_tasks(
        [Task("t", flaky)], RetryPolicy(retries=3, base_delay_seconds=1.5), waits.append
    )

    assert attempts == {"t": 3}
    assert waits == [1.5, 3.0]


def test_task_failing_beyond_retries_raises_and_stops_the_chain() -> None:
    later = Flaky(failures=0)
    tasks = [Task("first", Flaky(failures=5)), Task("second", later)]

    with pytest.raises(OSError, match="transient"):
        run_tasks(tasks, RetryPolicy(retries=1, base_delay_seconds=0), lambda _: None)
    assert later.calls == 0


def test_no_retry_exceptions_fail_immediately() -> None:
    calls = 0

    def gate() -> None:
        nonlocal calls
        calls += 1
        raise ValueError("gate failed")

    with pytest.raises(ValueError, match="gate failed"):
        run_tasks(
            [Task("gate", gate, no_retry=(ValueError,))],
            RetryPolicy(retries=3, base_delay_seconds=0),
            lambda _: None,
        )
    assert calls == 1


def test_no_retries_by_default() -> None:
    flaky = Flaky(failures=1)

    with pytest.raises(OSError, match="transient"):
        run_tasks([Task("t", flaky)])
    assert flaky.calls == 1


class FakeClock:
    """A clock that advances only when a task calls ``tick``, so durations are exact."""

    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now

    def tick(self, seconds: float) -> None:
        self.now += seconds


def test_each_task_reports_its_duration_and_outcome() -> None:
    clock = FakeClock()
    runs: list[TaskRun] = []
    tasks = [Task("fast", lambda: clock.tick(0.5)), Task("slow", lambda: clock.tick(4.0))]

    run_tasks(tasks, on_task_end=runs.append, clock=clock)

    assert runs == [TaskRun("fast", True, 1, 0.5), TaskRun("slow", True, 1, 4.0)]


def test_duration_adds_up_attempts_but_not_backoff_waits() -> None:
    clock = FakeClock()
    calls = 0

    def flaky() -> None:
        nonlocal calls
        calls += 1
        clock.tick(1.0)
        if calls < 3:
            raise OSError("transient")

    runs: list[TaskRun] = []
    run_tasks(
        [Task("t", flaky)],
        RetryPolicy(retries=2, base_delay_seconds=10),
        sleep=clock.tick,  # the wait advances the clock, yet must not be counted
        on_task_end=runs.append,
        clock=clock,
    )

    assert runs == [TaskRun("t", True, 3, 3.0)]


def test_failed_tasks_are_reported_before_the_error_propagates() -> None:
    clock = FakeClock()
    runs: list[TaskRun] = []

    def gate() -> None:
        clock.tick(2.0)
        raise ValueError("gate failed")

    def broken() -> None:
        clock.tick(0.25)
        raise OSError("disk")

    with pytest.raises(ValueError):
        run_tasks(
            [Task("a", lambda: clock.tick(1.0)), Task("gate", gate, no_retry=(ValueError,))],
            on_task_end=runs.append,
            clock=clock,
        )
    with pytest.raises(OSError):
        run_tasks(
            [Task("broken", broken)],
            RetryPolicy(retries=1, base_delay_seconds=0),
            lambda _: None,
            on_task_end=runs.append,
            clock=clock,
        )

    assert runs == [
        TaskRun("a", True, 1, 1.0),
        TaskRun("gate", False, 1, 2.0),
        TaskRun("broken", False, 2, 0.5),
    ]


def test_task_run_serialises_status_and_rounds_duration() -> None:
    assert TaskRun("silver", False, 2, 1.23456).as_dict() == {
        "name": "silver",
        "status": "failed",
        "attempts": 2,
        "duration_seconds": 1.235,
    }


@pytest.mark.parametrize("kwargs", [{"retries": -1}, {"base_delay_seconds": -0.1}])
def test_invalid_policy_is_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="negative"):
        RetryPolicy(**kwargs)


def test_cli_rejects_negative_retries(tmp_path) -> None:
    with pytest.raises(SystemExit):
        main(["run", "--data-dir", str(tmp_path), "--retries", "-1"])
