import importlib.util
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "backfill", Path(__file__).parents[2] / "scripts" / "backfill.py"
)
backfill = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(backfill)


def test_loops_every_hour_inclusive(monkeypatch):
    calls = []
    monkeypatch.setattr(backfill, "run_hour", lambda ts: calls.append(ts) or {"rows_written": 1})
    rc = backfill.main(["backfill.py", "2024-01-01", "2024-01-02"])
    assert len(calls) == 25
    assert rc == 0


def test_failure_skips_and_continues_and_exit_1(monkeypatch):
    def flaky(ts):
        if ts.hour == 1:
            raise RuntimeError("boom")
        return {"rows_written": 0}

    monkeypatch.setattr(backfill, "run_hour", flaky)
    rc = backfill.main(["backfill.py", "2024-01-01T00:00", "2024-01-01T03:00"])
    assert rc == 1


def test_bad_argv_returns_2():
    assert backfill.main(["backfill.py"]) == 2
