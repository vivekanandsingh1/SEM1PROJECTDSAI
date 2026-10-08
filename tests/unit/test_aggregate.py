import gzip
import json
import pathlib

import duckdb

from ghpulse.aggregate import build_serving
from ghpulse.transform import to_parquet

COLUMNS = {"date", "repo_name", "push_count", "pr_count", "comment_count", "unique_contributors"}


def _make_json_gz(path: pathlib.Path, events: list[dict]) -> str:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")
    return str(path)


def _event(
    n: int, event_type: str = "PushEvent", actor: str = "user0", repo: str = "org/repo"
) -> dict:
    return {
        "id": str(n),
        "type": event_type,
        "actor": {"login": actor},
        "repo": {"name": repo},
        "created_at": "2024-01-15T14:00:00Z",
        "payload": {"action": "opened" if event_type != "PushEvent" else None},
        "org": None,
    }


def _make_curated(tmp_path: pathlib.Path, events: list[dict], name: str = "curated.parquet") -> str:
    gz = tmp_path / f"{name}.gz"
    parquet = tmp_path / name
    _make_json_gz(gz, events)
    to_parquet(str(gz), str(parquet))
    return str(parquet)


def test_happy_path_returns_row_count(tmp_path):
    events = [_event(i) for i in range(20)]
    src = _make_curated(tmp_path, events)
    dst = str(tmp_path / "serving.parquet")
    result = build_serving(src, dst)
    assert result >= 1


def test_output_schema_columns(tmp_path):
    events = [_event(i) for i in range(20)]
    src = _make_curated(tmp_path, events)
    dst = str(tmp_path / "serving.parquet")
    build_serving(src, dst)
    con = duckdb.connect()
    cols = {
        row[0]
        for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [dst]).fetchall()
    }
    assert cols == COLUMNS


def test_count_arithmetic(tmp_path):
    events = (
        [_event(i, "PushEvent", "userA", "org/repo") for i in range(5)]
        + [_event(i + 5, "PullRequestEvent", "userA", "org/repo") for i in range(3)]
        + [_event(i + 8, "IssueCommentEvent", "userB", "org/repo") for i in range(2)]
    )
    src = _make_curated(tmp_path, events)
    dst = str(tmp_path / "serving.parquet")
    build_serving(src, dst)
    con = duckdb.connect()
    row = con.execute(
        "SELECT push_count, pr_count, comment_count, unique_contributors "
        "FROM read_parquet(?) WHERE repo_name = 'org/repo'",
        [dst],
    ).fetchone()
    assert row == (5, 3, 2, 2)


def test_multi_file_rollup(tmp_path):
    src_dir = tmp_path / "curated"
    src_dir.mkdir()
    dst_dir = tmp_path / "out"
    dst_dir.mkdir()
    events1 = [_event(i, "PushEvent", "userA", "org/repo") for i in range(3)]
    events2 = [_event(i + 10, "PushEvent", "userB", "org/repo") for i in range(2)]
    _make_curated(src_dir, events1, "hour0.parquet")
    _make_curated(src_dir, events2, "hour1.parquet")
    glob = str(src_dir / "*.parquet")
    dst = str(dst_dir / "serving.parquet")
    result = build_serving(glob, dst)
    assert result >= 1
    con = duckdb.connect()
    row = con.execute(
        "SELECT push_count, unique_contributors FROM read_parquet(?) WHERE repo_name = 'org/repo'",
        [dst],
    ).fetchone()
    assert row == (5, 2)


def test_empty_glob_returns_zero(tmp_path):
    glob = str(tmp_path / "nonexistent" / "*.parquet")
    dst = str(tmp_path / "serving.parquet")
    result = build_serving(glob, dst)
    assert result == 0
