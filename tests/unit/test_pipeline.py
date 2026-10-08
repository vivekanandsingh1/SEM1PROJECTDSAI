from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

from ghpulse.pipeline import run_day, run_hour

PHASES = (
    "ghpulse.extract.download_hour",
    "ghpulse.transform.to_parquet",
    "ghpulse.aggregate.build_serving",
    "ghpulse.load.publish_to_postgres",
)


def _patch_phases():
    patchers = [patch(target) for target in PHASES]
    mocks = [p.start() for p in patchers]
    m_dl, m_tr, m_agg, m_load = mocks
    m_dl.return_value = ("/fake/events.json.gz", 12345)
    m_tr.return_value = (1000, 999)
    m_agg.return_value = 42
    m_load.return_value = 7
    return patchers, {"dl": m_dl, "tr": m_tr, "agg": m_agg, "load": m_load}


def test_run_hour_calls_phases_in_order():
    patchers, m = _patch_phases()
    try:
        manager = MagicMock()
        manager.attach_mock(m["dl"], "download")
        manager.attach_mock(m["tr"], "transform")
        manager.attach_mock(m["agg"], "aggregate")
        manager.attach_mock(m["load"], "load")
        run_hour(datetime(2024, 1, 15, 14))
        names = [c[0] for c in manager.mock_calls]
        assert names == ["download", "transform", "aggregate", "load"]
    finally:
        for p in patchers:
            p.stop()


def test_run_hour_passes_cumulative_day_glob():
    patchers, m = _patch_phases()
    try:
        run_hour(datetime(2024, 1, 15, 14))
        glob_arg = m["agg"].call_args.args[0].replace("\\", "/")
        assert "/2024-01-15/*/events.parquet" in glob_arg
    finally:
        for p in patchers:
            p.stop()


def test_run_hour_result_shape():
    patchers, m = _patch_phases()
    try:
        ts = datetime(2024, 1, 15, 14)
        result = run_hour(ts)
        assert result == {
            "ts": ts,
            "bytes_raw": 12345,
            "rows_written": 999,
            "serving_rows": 42,
            "rows_loaded": 7,
        }
    finally:
        for p in patchers:
            p.stop()


def test_run_hour_propagates_phase_error():
    patchers, m = _patch_phases()
    m["dl"].side_effect = RuntimeError("network down")
    try:
        with pytest.raises(RuntimeError, match="network down"):
            run_hour(datetime(2024, 1, 15, 14))
    finally:
        for p in patchers:
            p.stop()


def test_run_day_runs_24_hours():
    with patch("ghpulse.pipeline.run_hour") as m_run:
        m_run.return_value = {"rows_written": 10, "rows_loaded": 3}
        run_day(datetime(2024, 1, 15))
        assert m_run.call_count == 24
        hours = [c.args[0].hour for c in m_run.call_args_list]
        assert hours == list(range(24))


def test_run_day_records_failures_and_continues():
    def fake_run_hour(ts):
        if ts.hour == 13:
            raise RuntimeError("boom")
        return {"rows_written": 10, "rows_loaded": 3}

    with patch("ghpulse.pipeline.run_hour", side_effect=fake_run_hour):
        result = run_day(datetime(2024, 1, 15))
    assert result["hours_ok"] == 23
    assert len(result["errors"]) == 1
    assert result["errors"][0] == {"hour": 13, "error": "RuntimeError", "detail": "boom"}
    assert result["rows_written"] == 230
    assert result["rows_loaded"] == 3
    assert result["date"] == datetime(2024, 1, 15).date()


def test_run_day_empty_when_all_fail():
    with patch("ghpulse.pipeline.run_hour", side_effect=RuntimeError("boom")):
        result = run_day(datetime(2024, 1, 15))
    assert result["hours_ok"] == 0
    assert result["rows_loaded"] == 0
    assert result["rows_written"] == 0
    assert len(result["errors"]) == 24


def test_run_day_keyboardinterrupt_propagates():
    with patch("ghpulse.pipeline.run_hour", side_effect=KeyboardInterrupt):
        with pytest.raises(KeyboardInterrupt):
            run_day(datetime(2024, 1, 15))
