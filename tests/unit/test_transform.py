import gzip
import json
import pathlib

import duckdb
import pytest

import ghpulse.transform as t
from ghpulse.transform import to_parquet

FIXTURE = pathlib.Path(__file__).parent.parent / "fixtures" / "sample_events.json"

COLUMNS = {
    "id",
    "event_type",
    "actor_login",
    "repo_name",
    "created_at",
    "payload_action",
    "org_login",
}


def _make_json_gz(path: pathlib.Path, events: list[dict]) -> str:
    with gzip.open(path, "wt", encoding="utf-8") as fh:
        for e in events:
            fh.write(json.dumps(e) + "\n")
    return str(path)


def _load_fixture() -> list[dict]:
    return [json.loads(line) for line in FIXTURE.read_text().splitlines() if line.strip()]


def test_happy_path_row_count(tmp_path):
    events = _load_fixture()
    src = _make_json_gz(tmp_path / "src.json.gz", events)
    dst = str(tmp_path / "out.parquet")
    total, written = to_parquet(src, dst)
    assert total == 500
    assert written == 500


def test_output_schema_columns(tmp_path):
    events = _load_fixture()
    src = _make_json_gz(tmp_path / "src.json.gz", events)
    dst = str(tmp_path / "out.parquet")
    to_parquet(src, dst)
    con = duckdb.connect()
    cols = {
        row[0]
        for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [dst]).fetchall()
    }
    assert cols == COLUMNS


def test_empty_src_returns_zero_zero(tmp_path):
    src = _make_json_gz(tmp_path / "src.json.gz", [])
    dst = str(tmp_path / "out.parquet")
    total, written = to_parquet(src, dst)
    assert total == 0
    assert written == 0


def _event(n: int) -> dict:
    return {
        "id": str(n),
        "type": "PushEvent",
        "actor": {"login": f"user{n}"},
        "repo": {"name": f"user{n}/repo"},
        "created_at": "2024-01-15T14:00:00Z",
        "payload": {"action": "opened"},
        "org": None,
    }


def test_drop_ratio_exceeded_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(t, "MAX_DROP_RATIO", -1.0)
    events = [_event(i) for i in range(100)]
    src = _make_json_gz(tmp_path / "src.json.gz", events)
    dst = str(tmp_path / "out.parquet")
    with pytest.raises(ValueError, match="Drop ratio"):
        to_parquet(src, dst)


def test_drop_ratio_within_bounds_passes(tmp_path):
    events = [_event(i) for i in range(100)]
    src = _make_json_gz(tmp_path / "src.json.gz", events)
    dst = str(tmp_path / "out.parquet")
    total, written = to_parquet(src, dst)
    assert total == written == 100
