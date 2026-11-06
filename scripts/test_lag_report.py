"""Unit tests for lag_report.py, with a fake AdminClient. No broker needed."""
import json
from concurrent.futures import Future
from types import SimpleNamespace
from unittest.mock import MagicMock

import lag_report
import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

GROUP = "orders"
ARGS = ["lag_report.py", "--bootstrap-servers", "broker:9092", "--group", GROUP]


def done(value):
    future = Future()
    future.set_result(value)
    return future


def failed(code):
    future = Future()
    future.set_exception(KafkaException(KafkaError(code)))
    return future


def fake_admin(state="STABLE", committed=None, ends=None):
    """Committed is {partition: offset}, ends is {partition: log end offset}."""
    committed = {0: 5, 1: 20} if committed is None else committed
    ends = {0: 10, 1: 20} if ends is None else ends
    admin = MagicMock()
    admin.describe_consumer_groups.return_value = {
        GROUP: done(SimpleNamespace(state=SimpleNamespace(name=state)))}
    partitions = [TopicPartition("t", p, o) for p, o in committed.items()]
    admin.list_consumer_group_offsets.return_value = {
        GROUP: done(SimpleNamespace(topic_partitions=partitions))}
    admin.list_offsets.return_value = {
        TopicPartition("t", p): done(SimpleNamespace(offset=o)) for p, o in ends.items()}
    return admin


@pytest.fixture
def run(monkeypatch, capsys):
    """Run main() with each attempt getting the next fake client from the list."""
    monkeypatch.delenv("KAFKA_API_KEY", raising=False)
    monkeypatch.delenv("KAFKA_API_SECRET", raising=False)
    monkeypatch.setattr(lag_report.time, "sleep", lambda _: None)
    monkeypatch.setattr(lag_report.sys, "argv", ARGS)

    def _run(*admins):
        factory = MagicMock(side_effect=list(admins))
        monkeypatch.setattr(lag_report, "AdminClient", factory)
        try:
            code = lag_report.main()
        except SystemExit as exc:
            code = exc.code
        out = capsys.readouterr()
        return SimpleNamespace(code=code, out=out.out, err=out.err, clients=factory.call_count,
                               factory=factory)

    return _run


def test_reports_lag_per_partition_in_order(run):
    result = run(fake_admin(committed={1: 20, 0: 5}))
    assert result.code == 0
    assert json.loads(result.out) == [
        {"topic": "t", "partition": 0, "current_offset": 5, "log_end_offset": 10, "lag": 5},
        {"topic": "t", "partition": 1, "current_offset": 20, "log_end_offset": 20, "lag": 0},
    ]


def test_truncated_topic_never_gives_negative_lag(run):
    result = run(fake_admin(committed={0: 50}, ends={0: 10}))
    assert json.loads(result.out)[0]["lag"] == 0


def test_dead_group_is_not_found(run):
    result = run(fake_admin(state="DEAD"))
    assert (result.code, result.out) == (2, "")
    assert "does not exist" in result.err


def test_group_id_not_found_error_is_not_found(run):
    admin = fake_admin()
    admin.describe_consumer_groups.return_value = {GROUP: failed(KafkaError.GROUP_ID_NOT_FOUND)}
    assert run(admin).code == 2


def test_group_with_no_commits_prints_empty_list(run):
    result = run(fake_admin(committed={}))
    assert (result.code, json.loads(result.out)) == (0, [])


def test_missing_leader_is_retried_and_never_printed_as_zero_lag(run):
    down = {0: -1001, 1: 20}
    result = run(fake_admin(ends=down), fake_admin(ends=down), fake_admin(ends=down))
    assert (result.code, result.out, result.clients) == (1, "", 3)
    assert "partition leader unavailable" in result.err


def test_recovers_when_the_leader_returns(run):
    result = run(fake_admin(ends={0: -1001, 1: 20}), fake_admin())
    assert (result.code, result.clients) == (0, 2)
    assert json.loads(result.out)[0]["lag"] == 5


@pytest.mark.parametrize("code", [KafkaError._NOENT, KafkaError.COORDINATOR_NOT_AVAILABLE,
                                  KafkaError.NOT_COORDINATOR, KafkaError.COORDINATOR_LOAD_IN_PROGRESS])
def test_failover_errors_are_retried(run, code):
    admin = fake_admin()
    admin.describe_consumer_groups.return_value = {GROUP: failed(code)}
    result = run(admin, fake_admin())
    assert (result.code, result.clients) == (0, 2)


def test_each_attempt_uses_a_new_client(run):
    admins = [fake_admin() for _ in range(3)]
    for admin in admins:
        admin.describe_consumer_groups.return_value = {GROUP: failed(KafkaError._TRANSPORT)}
    result = run(*admins)
    assert (result.code, result.out, result.clients) == (1, "", 3)
    assert all(admin.describe_consumer_groups.call_count == 1 for admin in admins)


def test_bad_credentials_are_not_retried(run):
    admin = fake_admin()
    admin.describe_consumer_groups.return_value = {GROUP: failed(KafkaError._TIMED_OUT)}

    def login_rejected(_):
        on_error = result_factory.call_args.args[0]["error_cb"]
        on_error(KafkaError(KafkaError._AUTHENTICATION))

    admin.poll.side_effect = login_rejected
    result_factory = MagicMock(return_value=admin)
    lag_report.AdminClient = result_factory
    assert lag_report.main() == 1
    assert result_factory.call_count == 1


def test_other_errors_are_not_retried(run):
    admin = fake_admin()
    admin.describe_consumer_groups.return_value = {
        GROUP: failed(KafkaError.GROUP_AUTHORIZATION_FAILED)}
    result = run(admin)
    assert (result.code, result.clients) == (1, 1)


@pytest.mark.parametrize("name", ["KAFKA_API_KEY", "KAFKA_API_SECRET"])
def test_half_set_credentials_are_refused(run, monkeypatch, name):
    monkeypatch.setenv(name, "value")
    result = run()
    assert (result.code, result.clients) == (2, 0)
    assert "set both" in result.err


def test_credentials_turn_on_sasl_and_never_print(run, monkeypatch):
    monkeypatch.setenv("KAFKA_API_KEY", "key")
    monkeypatch.setenv("KAFKA_API_SECRET", "s3cret-value")
    result = run(fake_admin())
    config = result.factory.call_args.args[0]
    assert config["security.protocol"] == "SASL_SSL"
    assert config["sasl.password"] == "s3cret-value"
    assert "s3cret-value" not in result.out + result.err


def test_missing_arguments_never_create_a_client(run, monkeypatch):
    monkeypatch.setattr(lag_report.sys, "argv", ["lag_report.py"])
    result = run()
    assert (result.code, result.clients) == (2, 0)
