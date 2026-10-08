import datetime
import pathlib
from unittest.mock import MagicMock, patch

import duckdb
import psycopg
import pytest

from ghpulse.load import AUDIT_SQL, UPSERT_SQL, publish_to_postgres


def _make_serving(tmp_path: pathlib.Path, n: int = 3) -> str:
    dst = str(tmp_path / "serving.parquet")
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE t (
            date DATE, repo_name TEXT,
            push_count BIGINT, pr_count BIGINT,
            comment_count BIGINT, unique_contributors BIGINT
        )
    """)
    if n > 0:
        rows = [
            (datetime.date(2024, 1, 15), f"org/repo{i}", i, i, i, i)
            for i in range(n)
        ]
        con.executemany("INSERT INTO t VALUES (?, ?, ?, ?, ?, ?)", rows)
    con.execute("COPY t TO ? (FORMAT PARQUET)", [dst])
    return dst


def _mock_pg():
    mock_cur = MagicMock()
    mock_conn = MagicMock()
    mock_conn.__enter__.return_value = mock_conn
    mock_conn.cursor.return_value.__enter__.return_value = mock_cur
    return mock_conn, mock_cur


def test_happy_path_returns_row_count(tmp_path):
    src = _make_serving(tmp_path, n=3)
    mock_conn, mock_cur = _mock_pg()
    with patch("psycopg.connect", return_value=mock_conn):
        result = publish_to_postgres(src, "postgresql://fake")
    assert result == 3


def test_upsert_sql_contains_on_conflict():
    assert "ON CONFLICT" in UPSERT_SQL


def test_audit_row_written(tmp_path):
    src = _make_serving(tmp_path, n=2)
    mock_conn, mock_cur = _mock_pg()
    with patch("psycopg.connect", return_value=mock_conn):
        publish_to_postgres(src, "postgresql://fake")
    last_call = mock_cur.execute.call_args_list[-1]
    assert last_call.args[0] == AUDIT_SQL
    assert last_call.args[1] == [src, 2]


def test_empty_parquet_returns_zero(tmp_path):
    src = _make_serving(tmp_path, n=0)
    mock_conn, mock_cur = _mock_pg()
    with patch("psycopg.connect", return_value=mock_conn):
        result = publish_to_postgres(src, "postgresql://fake")
    assert result == 0
    last_call = mock_cur.execute.call_args_list[-1]
    assert last_call.args[0] == AUDIT_SQL
    assert last_call.args[1] == [src, 0]


def test_executemany_called_with_rows(tmp_path):
    src = _make_serving(tmp_path, n=2)
    mock_conn, mock_cur = _mock_pg()
    with patch("psycopg.connect", return_value=mock_conn):
        publish_to_postgres(src, "postgresql://fake")
    call_args = mock_cur.executemany.call_args
    assert call_args.args[0] == UPSERT_SQL
    rows = call_args.args[1]
    assert len(rows) == 2
    date_val, repo_name, push_count, pr_count, comment_count, unique_contributors = rows[0]
    assert repo_name == "org/repo0"
    assert push_count == 0


def test_missing_src_raises(tmp_path):
    with pytest.raises(duckdb.IOException):
        publish_to_postgres(str(tmp_path / "nonexistent.parquet"), "postgresql://fake")


def test_pg_connection_error_propagates(tmp_path):
    src = _make_serving(tmp_path, n=1)
    with patch("psycopg.connect", side_effect=psycopg.OperationalError("bad url")):
        with pytest.raises(psycopg.OperationalError):
            publish_to_postgres(src, "postgresql://bad")
