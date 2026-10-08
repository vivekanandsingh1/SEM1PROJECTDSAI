# GH Pulse — GitHub Archive Batch ETL Pipeline

[![CI](https://github.com/vivekanandsingh1/SEM1PROJECTDSAI/actions/workflows/ci.yml/badge.svg)](https://github.com/vivekanandsingh1/SEM1PROJECTDSAI/actions/workflows/ci.yml)
[![Live Demo](https://img.shields.io/badge/Live%20Demo-Streamlit-FF4B4B?logo=streamlit&logoColor=white)](https://sem1dsaiproject.streamlit.app/)

**🔴 Live dashboard:** https://sem1dsaiproject.streamlit.app/

A production-grade batch ETL pipeline that ingests GitHub Archive data, transforms it with DuckDB, and serves analytics out of Postgres. Built phase by phase from a working foundation to a deployable system.

---

## What This Is

GitHub generates ~1M+ events per hour — pushes, pull requests, issues, comments, releases. [GHArchive](https://www.gharchive.org/) records every event as a compressed JSON file, one per hour, going back to 2011.

GH Pulse downloads those files, parses them into a columnar lake, aggregates developer activity metrics, and loads them into a queryable warehouse. The end result: a dashboard showing repository health, contributor trends, and community engagement over time.

This is **not** a D-tier project:

| D-tier                        | GH Pulse                                         |
|-------------------------------|--------------------------------------------------|
| Read one CSV                  | Downloads 8,784 gzipped JSON files per year      |
| Pandas transform              | DuckDB columnar transform on 100M+ rows          |
| Load to Postgres once         | Idempotent incremental loads with audit trail     |
| No error handling             | Retry logic, truncation detection, drop-ratio guards |
| No tests                      | 15+ unit tests, integration tests, CI             |
| Run manually                  | Scheduled, observable, re-runnable               |

---

## Tech Stack

| Layer             | Technology          | Why                                                                 |
|-------------------|---------------------|---------------------------------------------------------------------|
| Language          | Python 3.11+        | Industry standard for data engineering                              |
| HTTP Client       | httpx               | Streaming downloads, async-ready, better than requests              |
| Retry Logic       | tenacity            | Declarative retry policies, handles backoff automatically           |
| Transformation    | DuckDB              | Reads Parquet natively, SQL interface, no JVM, faster than Pandas   |
| Lake Format       | Parquet             | Columnar, compressed, DuckDB reads it directly                      |
| Warehouse         | PostgreSQL          | Reliable, queryable, industry standard OLTP/OLAP                    |
| Testing           | pytest              | Standard Python test runner                                         |
| Linting           | ruff                | Fast, replaces flake8 + isort + black                               |
| CI                | GitHub Actions      | Free for public repos, runs on every push                           |
| Packaging         | pyproject.toml      | Modern Python packaging standard                                    |

---

## Architecture

```
GHArchive (HTTP)
      |
      v
[Extract]  download_hour()
      |      - streams .json.gz to disk
      |      - validates gzip integrity
      |      - retries on network errors
      v
 data/lake/raw/YYYY-MM-DD/HH/events.json.gz
      |
      v
[Transform]  to_parquet()
      |      - DuckDB reads gzipped JSON directly
      |      - unnests nested event payloads
      |      - drops malformed rows (guarded by drop-ratio check)
      |      - writes columnar Parquet
      v
 data/lake/curated/YYYY-MM-DD/HH/events.parquet
      |
      v
[Aggregate]  build_serving()
      |      - DuckDB aggregates across hours
      |      - repo stats, contributor counts, event type breakdowns
      v
 data/serving/metrics.parquet
      |
      v
[Load]  publish_to_postgres()
      |      - COPY from Parquet into Postgres staging table
      |      - upsert into final table
      |      - writes audit log row (rows loaded, bytes, timestamp)
      v
 PostgreSQL  ghpulse.events  ghpulse.repo_metrics  ghpulse.audit_log
```

---

## Project Layout

```
ghpulse/
├── src/
│   └── ghpulse/
│       ├── __init__.py
│       ├── config.py          # env-var config, path helpers
│       ├── extract.py         # download_hour() — HTTP → disk
│       ├── transform.py       # to_parquet() — JSON.gz → Parquet
│       ├── aggregate.py       # build_serving() — Parquet → metrics Parquet
│       ├── publish.py         # publish_to_postgres() — Parquet → Postgres
│       └── pipeline.py        # run_pipeline() — orchestrates all phases
├── tests/
│   ├── unit/
│   │   ├── test_extract.py
│   │   ├── test_transform.py
│   │   ├── test_aggregate.py
│   │   └── test_publish.py
│   ├── integration/
│   │   └── test_pipeline_e2e.py
│   └── fixtures/
│       └── sample_events.json
├── scripts/
│   ├── verify_phase3.py       # manual integration test (network required)
│   └── backfill.py            # backfill historical hours
├── docs/
│   └── data-dictionary.md     # schema, null rates, field notes
├── .github/
│   └── workflows/
│       └── ci.yml             # GitHub Actions CI
├── pyproject.toml
├── .env.example
└── README.md
```

---

## Environment Variables

```bash
# .env.example
GHPULSE_LAKE_PATH=data/lake
GHPULSE_SERVING_PATH=data/serving
GHPULSE_AUDIT_PATH=data/audit
GHPULSE_WAREHOUSE_URL=postgresql://localhost:5432/ghpulse
```

Copy `.env.example` to `.env` and fill in values. The config module reads these lazily at call time, not at import time — this means tests can override them via fixtures without import-order issues.

---

## Phase-by-Phase Build Plan

### Phase 1 — Project Skeleton

**Goal:** Runnable Python package, linting passes, tests can be discovered.

**Steps:**

1. Create `pyproject.toml`:
   ```toml
   [build-system]
   requires = ["hatchling"]
   build-backend = "hatchling.build"

   [project]
   name = "ghpulse"
   version = "0.1.0"
   requires-python = ">=3.11"
   dependencies = [
       "httpx>=0.27",
       "tenacity>=8.3",
       "duckdb>=1.0",
       "psycopg[binary]>=3.1",
   ]

   [project.optional-dependencies]
   dev = ["pytest>=8", "ruff>=0.4"]

   [tool.ruff]
   line-length = 100
   ```

2. Create `src/ghpulse/__init__.py` (empty).

3. Create `tests/conftest.py` with env-var overrides pointing to temp directories.

4. Run `pip install -e ".[dev]"` to install in editable mode.

5. Run `ruff check .` — should pass on empty project.

6. Run `pytest` — should collect 0 tests and exit 0.

**Verify:** `python -c "import ghpulse"` runs without error.

---

### Phase 2 — Config

**Goal:** All paths and env vars read from one place. Lazy reading only — never at import time.

**Key design decision:** Config functions read `os.environ` when called, not when the module loads. This lets test fixtures set env vars before calling any config function. If you read at import time, the env var is captured before the fixture runs and tests break.

**Implementation:**

```python
# src/ghpulse/config.py
import os
from pathlib import Path
from datetime import datetime

GHARCHIVE_BASE_URL = "https://data.gharchive.org"

def _env(key: str) -> str:
    val = os.environ.get(key)
    if val is None:
        raise RuntimeError(f"Missing env var: {key}")
    return val

def lake_path() -> Path:
    return Path(_env("GHPULSE_LAKE_PATH"))

def raw_path(dt: datetime, hour: int) -> Path:
    return lake_path() / "raw" / dt.strftime("%Y-%m-%d") / f"{hour:02d}"

def curated_path(dt: datetime, hour: int) -> Path:
    return lake_path() / "curated" / dt.strftime("%Y-%m-%d") / f"{hour:02d}"

def serving_path() -> Path:
    return Path(_env("GHPULSE_SERVING_PATH"))

def warehouse_url() -> str:
    return _env("GHPULSE_WAREHOUSE_URL")
```

**Test:** One test that sets the env var and checks `raw_path()` returns the correct `Path`.

---

### Phase 3 — Extract (Resilient Ingestion) ✅ COMPLETE

**Goal:** Download one hour of GHArchive data reliably. Handle network failures, truncated downloads, and corrupt files.

**What makes this non-trivial:**

- GHArchive files aren't published until ~5 minutes after the hour. A 404 means "not yet", not "missing" — retry it.
- Network connections drop mid-stream. A 0-byte file or a truncated gzip must be detected and retried, not silently passed downstream.
- Files can be corrupt (wrong magic bytes). That's not a transient error — don't retry it.

**Key implementation details:**

```python
# src/ghpulse/extract.py

@retry(
    retry=retry_if_exception(_should_retry),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def download_hour(ts: datetime, dest: Path) -> tuple[Path, int]:
    ...
```

- Uses `httpx.stream()` for streaming downloads — never loads the whole file into memory.
- Writes to a `.tmp` file, validates, then `rename()` to final path (atomic on most filesystems).
- `.tmp` file is always deleted on any exception — no partial files left behind.
- Content-Length check uses raw wire bytes (`iter_raw`), not decompressed bytes (`iter_bytes`).

**Tests (15 total):** Happy path, idempotent re-run, corrupt gzip, 500 retry, retry exhaustion, 403 no retry, 404 as NotPublishedYet, mid-stream read error, truncated gzip, zero-byte body, empty gzip stream, Content-Length mismatch, Content-Encoding false positive.

**Status:** Done. All 15 tests pass.

---

### Phase 4 — Transform (JSON.gz → Parquet)

**Goal:** Parse raw JSON events into typed columnar Parquet. Validate data quality. Log how many rows were dropped.

**Why DuckDB instead of Pandas:**

- DuckDB reads `.json.gz` directly — no Python decompression loop needed.
- DuckDB's JSON reader handles nested structures natively.
- On 100MB+ files, DuckDB is 5–20x faster than Pandas for this kind of work.
- Parquet output integrates with the next DuckDB aggregation step without loading into memory.

**Schema for curated Parquet:**

| Column              | Type        | Source                           |
|---------------------|-------------|----------------------------------|
| `id`                | VARCHAR     | `event.id`                       |
| `event_type`        | VARCHAR     | `event.type`                     |
| `actor_login`       | VARCHAR     | `event.actor.login`              |
| `repo_name`         | VARCHAR     | `event.repo.name`                |
| `created_at`        | TIMESTAMP   | `event.created_at`               |
| `payload_action`    | VARCHAR     | `event.payload.action` (nullable)|
| `org_login`         | VARCHAR     | `event.org.login` (nullable)     |

**Implementation sketch:**

```python
# src/ghpulse/transform.py
import duckdb
from pathlib import Path

MAX_DROP_RATIO = 0.001  # fail if >0.1% of rows dropped

COUNT_SQL = "SELECT COUNT(*) FROM read_json_auto(?)"
COPY_SQL = """
COPY (
    SELECT
        id,
        type        AS event_type,
        actor.login AS actor_login,
        repo.name   AS repo_name,
        created_at::TIMESTAMP AS created_at,
        payload.action AS payload_action,
        org.login   AS org_login
    FROM read_json_auto(?, ignore_errors=true)
) TO ? (FORMAT PARQUET)
"""

def to_parquet(src: str, dst: str) -> tuple[int, int]:
    con = duckdb.connect()
    total = con.execute(COUNT_SQL, [src]).fetchone()[0]
    con.execute(COPY_SQL, [src, src, dst])
    written = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [dst]).fetchone()[0]
    dropped = total - written
    if total > 0 and dropped / total > MAX_DROP_RATIO:
        raise ValueError(f"Drop ratio {dropped/total:.4%} exceeds {MAX_DROP_RATIO:.4%}")
    return total, written
```

**Tests to write:**
- Happy path: sample JSON → Parquet, check row count.
- Drop ratio exceeded: inject malformed rows beyond threshold, expect `ValueError`.
- Drop ratio within bounds: a few bad rows, function succeeds.
- Schema check: output Parquet has all expected columns with correct types.

---

### Phase 5 — Aggregate (Parquet → Serving Metrics)

**Goal:** Roll up per-hour Parquet files into daily/weekly aggregates suitable for dashboards.

**Metrics to compute:**

```sql
-- Per-repo per-day metrics
SELECT
    DATE_TRUNC('day', created_at) AS date,
    repo_name,
    COUNT(*) FILTER (WHERE event_type = 'PushEvent')           AS push_count,
    COUNT(*) FILTER (WHERE event_type = 'PullRequestEvent')    AS pr_count,
    COUNT(*) FILTER (WHERE event_type = 'IssueCommentEvent')   AS comment_count,
    COUNT(DISTINCT actor_login)                                 AS unique_contributors
FROM read_parquet('data/lake/curated/**/*.parquet')
GROUP BY 1, 2
```

**Implementation:**

```python
# src/ghpulse/aggregate.py
import duckdb
from pathlib import Path

AGGREGATE_SQL = """
COPY (
    SELECT
        DATE_TRUNC('day', created_at)::DATE AS date,
        repo_name,
        COUNT(*) FILTER (WHERE event_type = 'PushEvent') AS push_count,
        COUNT(*) FILTER (WHERE event_type = 'PullRequestEvent') AS pr_count,
        COUNT(*) FILTER (WHERE event_type = 'IssueCommentEvent') AS comment_count,
        COUNT(DISTINCT actor_login) AS unique_contributors
    FROM read_parquet(?)
    GROUP BY 1, 2
) TO ? (FORMAT PARQUET)
"""

def build_serving(curated_glob: str, dst: str) -> int:
    con = duckdb.connect()
    con.execute(AGGREGATE_SQL, [curated_glob, dst])
    return con.execute("SELECT COUNT(*) FROM read_parquet(?)", [dst]).fetchone()[0]
```

---

### Phase 6 — Load (Parquet → PostgreSQL)

**Goal:** Load serving metrics into Postgres. Support incremental loads — re-running the same day should upsert, not duplicate. Log every load with row counts and timestamps.

**Postgres schema:**

```sql
CREATE TABLE IF NOT EXISTS repo_metrics (
    date                DATE        NOT NULL,
    repo_name           TEXT        NOT NULL,
    push_count          BIGINT      DEFAULT 0,
    pr_count            BIGINT      DEFAULT 0,
    comment_count       BIGINT      DEFAULT 0,
    unique_contributors BIGINT      DEFAULT 0,
    PRIMARY KEY (date, repo_name)
);

CREATE TABLE IF NOT EXISTS audit_log (
    id          SERIAL      PRIMARY KEY,
    run_at      TIMESTAMP   DEFAULT NOW(),
    rows_loaded BIGINT,
    bytes_read  BIGINT,
    status      TEXT
);
```

**Implementation:**

```python
# src/ghpulse/publish.py
import psycopg
import duckdb
from pathlib import Path

UPSERT_SQL = """
INSERT INTO repo_metrics (date, repo_name, push_count, pr_count, comment_count, unique_contributors)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (date, repo_name) DO UPDATE SET
    push_count          = EXCLUDED.push_count,
    pr_count            = EXCLUDED.pr_count,
    comment_count       = EXCLUDED.comment_count,
    unique_contributors = EXCLUDED.unique_contributors
"""

def publish_to_postgres(serving_path: str, warehouse_url: str) -> int:
    rows = duckdb.connect().execute(
        "SELECT * FROM read_parquet(?)", [serving_path]
    ).fetchall()
    with psycopg.connect(warehouse_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, rows)
            cur.execute(
                "INSERT INTO audit_log (rows_loaded, status) VALUES (%s, %s)",
                (len(rows), "ok")
            )
    return len(rows)
```

**Idempotency:** The `ON CONFLICT ... DO UPDATE` upsert means running the same day twice is safe — it overwrites with the same values instead of inserting duplicates.

---

### Phase 7 — Pipeline Orchestration

**Goal:** One function that runs all phases for a given datetime range. Handles partial failures gracefully.

```python
# src/ghpulse/pipeline.py
from datetime import datetime, timedelta
from pathlib import Path
from ghpulse import config, extract, transform, aggregate, publish

def run_hour(ts: datetime) -> dict:
    hour = ts.hour
    raw_dest = config.raw_path(ts, hour) / "events.json.gz"
    curated_dest = config.curated_path(ts, hour) / "events.parquet"

    raw_dest.parent.mkdir(parents=True, exist_ok=True)
    curated_dest.parent.mkdir(parents=True, exist_ok=True)

    path, bytes_raw = extract.download_hour(ts, raw_dest)
    total, written = transform.to_parquet(str(path), str(curated_dest))

    return {"ts": ts, "bytes_raw": bytes_raw, "rows_written": written}

def run_day(date: datetime) -> list[dict]:
    return [run_hour(date.replace(hour=h)) for h in range(24)]
```

**Usage:**

```bash
python -c "
from datetime import datetime
from ghpulse.pipeline import run_day
results = run_day(datetime(2024, 1, 15))
print(f'Loaded {sum(r[\"rows_written\"] for r in results):,} rows')
"
```

---

### Phase 8 — CI with GitHub Actions

**Goal:** Every push runs linting and tests. PRs can't merge if tests fail.

```yaml
# .github/workflows/ci.yml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.11"
      - run: pip install -e ".[dev]"
      - run: ruff check .
      - run: pytest tests/unit/ -v
```

**What this proves to recruiters:** You didn't just write code — you built a system that validates itself automatically. Any change that breaks a test fails loudly before it gets merged.

---

### Phase 9 — Backfill Script

**Goal:** Download and process multiple months of historical data in one command.

```python
# scripts/backfill.py
"""
Usage: python scripts/backfill.py 2024-01-01 2024-03-31
"""
import sys
from datetime import datetime, timedelta
from ghpulse.pipeline import run_hour

start = datetime.fromisoformat(sys.argv[1])
end   = datetime.fromisoformat(sys.argv[2])

current = start
while current <= end:
    try:
        result = run_hour(current)
        print(f"OK  {current.isoformat()}  {result['rows_written']:,} rows")
    except Exception as e:
        print(f"ERR {current.isoformat()}  {e}", file=sys.stderr)
    current += timedelta(hours=1)
```

Run with a date range to fill the warehouse. Failed hours print to stderr and are skipped — you can re-run just those hours.

---

## Key Engineering Decisions

### Why not Airflow or Prefect?

For a semester project running on a laptop, a `for` loop over hours is enough. Airflow adds a web server, a metadata DB, a scheduler process, and a worker pool. All of that for a batch job that runs once a day is over-engineering. Add an orchestrator when you need parallelism, retries with UI, or multiple pipelines sharing a scheduler.

### Why Parquet instead of CSV?

Parquet is columnar — DuckDB can read only the columns it needs without scanning the whole file. A CSV with 10 columns and 10M rows requires reading all 10 columns even if your query only touches 2. Parquet also stores type information (timestamps as int64, not strings), so no parsing surprises.

### Why not Spark?

Spark distributes work across a cluster. For 1GB/hour of JSON, a single DuckDB process running in-memory is faster than Spark's coordination overhead. Spark makes sense at 1TB+.

### Why upsert instead of truncate-reload?

Truncate-reload is simpler, but it creates a window where the table has 0 rows between the DELETE and the INSERT. Any query running during that window returns empty results. Upsert is slightly more complex but keeps the table queryable at all times.

---

## Running Locally

```bash
# 1. Clone and install
git clone <your-repo>
cd ghpulse
pip install -e ".[dev]"

# 2. Set env vars
cp .env.example .env
# edit .env with your Postgres connection string

# 3. Run unit tests
pytest tests/unit/ -v

# 4. Run one hour (requires network)
python -c "
from datetime import datetime
from ghpulse.pipeline import run_hour
result = run_hour(datetime(2024, 1, 15, 14))
print(result)
"

# 5. Check linting
ruff check .
```

---

## Progress Tracker

- [x] Phase 1 — Project Skeleton
- [x] Phase 2 — Config
- [x] Phase 3 — Extract (15 unit tests, all passing)
- [x] Phase 4 — Transform
- [x] Phase 5 — Aggregate
- [x] Phase 6 — Load
- [x] Phase 7 — Pipeline Orchestration
- [x] Phase 8 — CI
- [x] Phase 9 — Backfill Script

---

## What Makes This Portfolio-Worthy

- **Resilience:** Retry logic with exponential backoff, truncation detection, atomic file writes.
- **Data quality:** Drop-ratio guard prevents silently bad data from entering the warehouse.
- **Idempotency:** Re-running any phase produces the same result — no duplicates, no data loss.
- **Testability:** 15+ unit tests with mocked HTTP, no real network calls needed.
- **Observability:** Audit log records every load with row counts and timestamps.
- **CI:** GitHub Actions runs tests on every push.
- **Documentation:** Data dictionary, architecture diagram, per-phase decisions recorded.
