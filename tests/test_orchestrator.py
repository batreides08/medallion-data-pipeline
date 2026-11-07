import pytest

from medallion_pipeline.cli import main
from medallion_pipeline.orchestrator import RetryPolicy, Task, run_tasks


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


@pytest.mark.parametrize("kwargs", [{"retries": -1}, {"base_delay_seconds": -0.1}])
def test_invalid_policy_is_rejected(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match="negative"):
        RetryPolicy(**kwargs)


def test_cli_rejects_negative_retries(tmp_path) -> None:
    with pytest.raises(SystemExit):
        main(["run", "--data-dir", str(tmp_path), "--retries", "-1"])
