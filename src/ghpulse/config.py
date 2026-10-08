import os
from datetime import datetime
from pathlib import Path

GHARCHIVE_BASE_URL = "https://data.gharchive.org"


def _env(key: str) -> str:
    val = os.environ.get(key)
    if val is None:
        raise RuntimeError(f"Missing env var: {key}")
    return val


def lake_path() -> Path:
    return Path(_env("GHPULSE_LAKE_PATH"))


def warehouse_url() -> str:
    return _env("GHPULSE_WAREHOUSE_URL")


def raw_path(dt: datetime, hour: int) -> Path:
    return lake_path() / "raw" / dt.strftime("%Y-%m-%d") / f"{hour:02d}"


def curated_path(dt: datetime, hour: int) -> Path:
    return lake_path() / "curated" / dt.strftime("%Y-%m-%d") / f"{hour:02d}"


def serving_path() -> Path:
    return Path(_env("GHPULSE_SERVING_PATH"))


def audit_path(dt: datetime, hour: int) -> Path:
    return Path(_env("GHPULSE_AUDIT_PATH")) / dt.strftime("%Y-%m-%d") / f"{hour:02d}"
