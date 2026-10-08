import duckdb
import psycopg

METRICS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS repo_daily_metrics (
    date                DATE    NOT NULL,
    repo_name           TEXT    NOT NULL,
    push_count          BIGINT  NOT NULL DEFAULT 0,
    pr_count            BIGINT  NOT NULL DEFAULT 0,
    comment_count       BIGINT  NOT NULL DEFAULT 0,
    unique_contributors BIGINT  NOT NULL DEFAULT 0,
    PRIMARY KEY (date, repo_name)
)
"""

AUDIT_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS load_audit (
    id        SERIAL      PRIMARY KEY,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    src_path  TEXT        NOT NULL,
    row_count INT         NOT NULL
)
"""

UPSERT_SQL = """
INSERT INTO repo_daily_metrics
    (date, repo_name, push_count, pr_count, comment_count, unique_contributors)
VALUES (%s, %s, %s, %s, %s, %s)
ON CONFLICT (date, repo_name) DO UPDATE SET
    push_count          = EXCLUDED.push_count,
    pr_count            = EXCLUDED.pr_count,
    comment_count       = EXCLUDED.comment_count,
    unique_contributors = EXCLUDED.unique_contributors
"""

AUDIT_SQL = """
INSERT INTO load_audit (src_path, row_count) VALUES (%s, %s)
"""


def publish_to_postgres(src: str, conn_url: str) -> int:
    con = duckdb.connect()
    rows = con.execute(
        "SELECT date, repo_name, push_count, pr_count, comment_count, unique_contributors "
        "FROM read_parquet(?)",
        [src],
    ).fetchall()

    with psycopg.connect(conn_url) as pg:
        with pg.cursor() as cur:
            cur.execute(METRICS_TABLE_SQL)
            cur.execute(AUDIT_TABLE_SQL)
            cur.executemany(UPSERT_SQL, rows)
            cur.execute(AUDIT_SQL, [src, len(rows)])

    return len(rows)
