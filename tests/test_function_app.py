"""Tests for function_app.py, runnable without Azure, pyodbc or a network.

`azure.functions` and `pyodbc` are replaced by small stand-ins before the module
is imported, because neither is installable on every machine that runs this
suite and neither is what the tests are about. What the tests pin is the
behaviour that cost real data: a duplicate must never roll back the rows before
it, the ingestion count must be rows written and not rows attempted, the write
endpoint must not be anonymous, an error body must not carry the driver's
message, and a transient Azure SQL error must be retried without doubling rows.
"""

import sys
import types
from unittest.mock import MagicMock

import pytest


class _PyodbcError(Exception):
    """Stands in for pyodbc.Error: args are (sqlstate, driver message)."""


class _IntegrityError(_PyodbcError):
    pass


def _sql_error(number):
    """A pyodbc-shaped error carrying SQL Server error `number` in its message."""
    return _PyodbcError(
        "42000", f"[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]Some failure. ({number}) (SQLDriverConnect)"
    )


class _AuthLevel:
    ANONYMOUS = "anonymous"
    FUNCTION = "function"


class _HttpResponse:
    def __init__(self, body=b"", status_code=200, mimetype=None, headers=None):
        self.body = body
        self.status_code = status_code
        self.mimetype = mimetype
        self.headers = headers or {}


class _FunctionApp:
    """Records every route's keyword arguments, so a test can read the auth level."""

    def __init__(self):
        self.routes = {}

    def route(self, route, **kwargs):
        self.routes[route] = kwargs
        return lambda fn: fn

    def timer_trigger(self, **kwargs):
        return lambda fn: fn


@pytest.fixture(scope="module")
def function_app():
    azure = types.ModuleType("azure")
    functions = types.ModuleType("azure.functions")
    functions.FunctionApp = _FunctionApp
    functions.AuthLevel = _AuthLevel
    functions.HttpResponse = _HttpResponse
    functions.HttpRequest = object
    functions.TimerRequest = object
    azure.functions = functions
    pyodbc = types.ModuleType("pyodbc")
    pyodbc.Error = _PyodbcError
    pyodbc.IntegrityError = _IntegrityError
    pyodbc.connect = lambda *a, **k: (_ for _ in ()).throw(RuntimeError("no database in tests"))
    sys.modules.update({"azure": azure, "azure.functions": functions, "pyodbc": pyodbc})
    sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parents[1]))
    import function_app  # noqa: E402

    return function_app


class _Cursor:
    """A cursor whose INSERT ... WHERE NOT EXISTS behaves like the real one.

    The dedup key is tracked in a set, so a repeated key reports `rowcount == 0`
    the way SQL Server does when the SELECT yields no row.
    """

    def __init__(self, raise_on=()):
        self.seen = set()
        self.rowcount = -1
        self.raise_on = set(raise_on)

    def execute(self, sql, *params):
        if "INSERT INTO liveboard_records" not in sql:
            self.rowcount = -1
            return
        key = (params[0], params[1], params[3], params[6])
        if key in self.raise_on:
            raise _IntegrityError("UNIQUE KEY constraint 'UQ_liveboard_dedup'")
        self.rowcount = 0 if key in self.seen else 1
        self.seen.add(key)

    def close(self):
        pass


def _insert(function_app, cursor, conn, suffix):
    return function_app.insert_liveboard_record(
        cursor, conn, "BE.NMBS.008812005", f"trip-{suffix}", "", f"2026-07-30T08:0{suffix}", 60, 0, "pull-1"
    )


def test_a_duplicate_does_not_roll_back_the_rows_before_it(function_app):
    """The real defect: rollback() on the shared connection undid the whole pull."""
    conn = MagicMock()
    cursor = _Cursor()
    assert _insert(function_app, cursor, conn, 1) is True
    assert _insert(function_app, cursor, conn, 1) is False      # same key again
    assert _insert(function_app, cursor, conn, 2) is True
    conn.rollback.assert_not_called()
    assert len(cursor.seen) == 2


def test_a_race_integrity_error_is_skipped_without_rollback(function_app):
    conn = MagicMock()
    cursor = _Cursor(raise_on={("BE.NMBS.008812005", "trip-9", "2026-07-30T08:09", "pull-1")})
    assert _insert(function_app, cursor, conn, 9) is False
    conn.rollback.assert_not_called()


def test_the_ingestion_count_is_rows_written_not_rows_attempted(function_app, monkeypatch):
    """Two stop-time updates with the same key must count once."""
    stu = {"stopId": "8812005_1", "departure": {"time": 1_753_862_400, "delay": 60}}
    feed = {"entity": [{"tripUpdate": {"trip": {"tripId": "IC-1"}, "stopTimeUpdate": [stu, dict(stu)]}}]}
    monkeypatch.setattr(function_app, "fetch_gtfs_rt_feed", lambda url: feed)
    monkeypatch.setattr(function_app, "load_known_station_ids", lambda cursor: {"8812005"})
    monkeypatch.setattr(function_app, "upsert_vehicle", lambda *a: None)
    assert function_app.process_trip_updates(_Cursor(), MagicMock()) == 1


def test_the_write_endpoint_requires_a_function_key(function_app):
    route = function_app.app.routes["fetch-liveboard"]
    assert route["auth_level"] == _AuthLevel.FUNCTION
    assert route["methods"] == ["POST"]


def test_read_endpoints_stay_anonymous(function_app):
    for name in ("station-stats", "network-leaderboard", "dashboard"):
        assert function_app.app.routes[name]["auth_level"] == _AuthLevel.ANONYMOUS


def test_an_error_body_carries_a_reference_not_the_driver_message(function_app, monkeypatch):
    def boom(**kwargs):
        raise RuntimeError("Login failed for user 'sa' on serv-railpulse")

    monkeypatch.setattr(function_app, "run_ingestion", boom)
    response = function_app.fetch_liveboard_http(object())
    assert response.status_code == 500
    assert "serv-railpulse" not in response.body
    assert "Reference" in response.body


def _flaky(errors, result="done"):
    """An operation that raises each of `errors` in turn, then returns `result`."""
    calls = []

    def operation():
        calls.append(1)
        if len(calls) <= len(errors):
            raise errors[len(calls) - 1]
        return result

    return operation, calls


@pytest.mark.parametrize("number", [40613, 40197, 40501, 49918, 49919, 49920, 4060, 4221])
def test_a_transient_sql_error_is_retried_with_backoff(function_app, number):
    operation, calls = _flaky([_sql_error(number), _sql_error(number)])
    delays = []
    assert function_app.with_sql_retry(operation, sleep=delays.append) == "done"
    assert len(calls) == 3
    assert delays == [5.0, 10.0]


def test_a_non_transient_sql_error_is_not_retried(function_app):
    operation, calls = _flaky([_sql_error(18456)])  # login failed: retrying won't fix it
    delays = []
    with pytest.raises(_PyodbcError):
        function_app.with_sql_retry(operation, sleep=delays.append)
    assert len(calls) == 1 and delays == []


def test_a_non_sql_error_is_not_retried(function_app):
    operation, calls = _flaky([ValueError("bad feed (40613)")])
    with pytest.raises(ValueError):
        function_app.with_sql_retry(operation, sleep=lambda s: None)
    assert len(calls) == 1


def test_the_backoff_is_capped_and_the_last_error_propagates(function_app):
    operation, calls = _flaky([_sql_error(40613)] * 10)
    delays = []
    with pytest.raises(_PyodbcError):
        function_app.with_sql_retry(operation, attempts=7, sleep=delays.append)
    assert len(calls) == 7
    assert delays == [5.0, 10.0, 20.0, 40.0, 60.0, 60.0]


def test_a_retried_ingestion_replays_the_same_pull_without_duplicates(function_app, monkeypatch):
    """The commit succeeds server-side but its acknowledgement is lost (40613).

    The retry must reuse the feed and pulled_at it already has: the rows are
    then all present, WHERE NOT EXISTS skips them, and nothing is doubled.
    """
    stus = [{"stopId": "8812005_1", "departure": {"time": 1_753_862_400 + 60 * i}} for i in range(3)]
    feed = {"entity": [{"tripUpdate": {"trip": {"tripId": "IC-1"}, "stopTimeUpdate": stus}}]}
    fetches = []
    monkeypatch.setattr(function_app, "fetch_gtfs_rt_feed", lambda url: fetches.append(url) or feed)
    monkeypatch.setattr(function_app, "load_known_station_ids", lambda cursor: {"8812005"})
    monkeypatch.setattr(function_app.time, "sleep", lambda s: None)

    table = _Cursor()  # one cursor = one table that outlives each connection
    connections = []

    def connect():
        conn = MagicMock()
        conn.cursor.return_value = table
        if not connections:
            conn.commit.side_effect = _sql_error(40613)
        connections.append(conn)
        return conn

    monkeypatch.setattr(function_app, "get_db_connection", connect)

    function_app.run_ingestion()

    assert len(fetches) == 1
    assert len(connections) == 2
    assert all(c.close.called for c in connections)
    assert len(table.seen) == 3


def test_the_http_endpoint_retries_less_than_the_timer(function_app, monkeypatch):
    # A caller waits on the HTTP request and Azure cuts it at 230 s, so the
    # endpoint asks for 2 attempts, not the full 5.
    calls = []
    monkeypatch.setattr(function_app, "run_ingestion", lambda **kw: calls.append(kw) or 7)
    response = function_app.fetch_liveboard_http(object())
    assert response.status_code == 200
    assert calls == [{"retry_attempts": 2}]


def test_run_ingestion_stops_after_its_retry_attempts(function_app, monkeypatch):
    monkeypatch.setattr(function_app, "fetch_gtfs_rt_feed", lambda url: {"entity": []})
    delays = []
    monkeypatch.setattr(function_app.time, "sleep", delays.append)
    connections = []

    def connect():
        connections.append(1)
        raise _sql_error(40613)

    monkeypatch.setattr(function_app, "get_db_connection", connect)
    with pytest.raises(_PyodbcError):
        function_app.run_ingestion(retry_attempts=2)
    assert len(connections) == 2
    assert delays == [5.0]
