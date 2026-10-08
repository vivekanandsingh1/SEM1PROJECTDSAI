from datetime import datetime

import pytest

from ghpulse.config import (
    GHARCHIVE_BASE_URL,
    audit_path,
    curated_path,
    raw_path,
    serving_path,
    warehouse_url,
)


def test_raw_path_structure(tmp_path, monkeypatch):
    monkeypatch.setenv("GHPULSE_LAKE_PATH", str(tmp_path))
    dt = datetime(2024, 1, 15)
    result = raw_path(dt, 7)
    assert result == tmp_path / "raw" / "2024-01-15" / "07"


def test_curated_path_structure(tmp_path, monkeypatch):
    monkeypatch.setenv("GHPULSE_LAKE_PATH", str(tmp_path))
    dt = datetime(2024, 1, 15)
    result = curated_path(dt, 14)
    assert result == tmp_path / "curated" / "2024-01-15" / "14"


def test_serving_path(tmp_path, monkeypatch):
    monkeypatch.setenv("GHPULSE_SERVING_PATH", str(tmp_path / "serving"))
    assert serving_path() == tmp_path / "serving"


def test_audit_path_structure(tmp_path, monkeypatch):
    monkeypatch.setenv("GHPULSE_AUDIT_PATH", str(tmp_path / "audit"))
    dt = datetime(2024, 3, 5)
    result = audit_path(dt, 0)
    assert result == tmp_path / "audit" / "2024-03-05" / "00"


def test_hour_zero_padded(tmp_path, monkeypatch):
    monkeypatch.setenv("GHPULSE_LAKE_PATH", str(tmp_path))
    result = raw_path(datetime(2024, 1, 1), 3)
    assert result.name == "03"


def test_missing_env_var_raises(monkeypatch):
    monkeypatch.delenv("GHPULSE_SERVING_PATH", raising=False)
    with pytest.raises(RuntimeError, match="Missing env var: GHPULSE_SERVING_PATH"):
        serving_path()


def test_gharchive_base_url():
    assert GHARCHIVE_BASE_URL == "https://data.gharchive.org"


def test_warehouse_url(monkeypatch):
    monkeypatch.setenv("GHPULSE_WAREHOUSE_URL", "postgresql://localhost:5432/ghpulse_test")
    assert warehouse_url() == "postgresql://localhost:5432/ghpulse_test"
