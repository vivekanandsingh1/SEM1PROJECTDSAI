import duckdb

MAX_DROP_RATIO = 0.001

COUNT_SQL = "SELECT COUNT(*) FROM read_json_auto(?)"

COPY_SQL = """
COPY (
    SELECT
        id,
        type        AS event_type,
        actor.login AS actor_login,
        repo.name   AS repo_name,
        created_at::TIMESTAMP AS created_at,
        payload.action        AS payload_action,
        org.login             AS org_login
    FROM read_json_auto(?, ignore_errors=true)
) TO ? (FORMAT PARQUET)
"""


def to_parquet(src: str, dst: str) -> tuple[int, int]:
    con = duckdb.connect()
    total = con.execute(COUNT_SQL, [src]).fetchone()[0]
    if total == 0:
        return 0, 0
    con.execute(COPY_SQL, [dst, src])
    written = con.execute("SELECT COUNT(*) FROM read_parquet(?)", [dst]).fetchone()[0]
    dropped = total - written
    if dropped / total > MAX_DROP_RATIO:
        raise ValueError(
            f"Drop ratio {dropped/total:.4%} exceeds MAX_DROP_RATIO {MAX_DROP_RATIO:.4%}"
        )
    return total, written
