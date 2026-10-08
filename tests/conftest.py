import os

import pytest

os.environ.setdefault("GHPULSE_LAKE_PATH", "/tmp/ghpulse-test/lake")
os.environ.setdefault("GHPULSE_SERVING_PATH", "/tmp/ghpulse-test/serving")
os.environ.setdefault("GHPULSE_AUDIT_PATH", "/tmp/ghpulse-test/audit")
os.environ.setdefault("GHPULSE_WAREHOUSE_URL", "postgresql://localhost:5432/ghpulse_test")


@pytest.fixture(autouse=True, scope="session")
def _test_env(tmp_path_factory):
    base = tmp_path_factory.mktemp("ghpulse")
    os.environ["GHPULSE_LAKE_PATH"] = str(base / "lake")
    os.environ["GHPULSE_SERVING_PATH"] = str(base / "serving")
    os.environ["GHPULSE_AUDIT_PATH"] = str(base / "audit")
