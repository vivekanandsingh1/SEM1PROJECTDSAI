import duckdb

ROLLUP_SQL = """
COPY (
    SELECT
        created_at::DATE         AS date,
        repo_name,
        COUNT(*) FILTER (WHERE event_type = 'PushEvent')          AS push_count,
        COUNT(*) FILTER (WHERE event_type = 'PullRequestEvent')   AS pr_count,
        COUNT(*) FILTER (WHERE event_type = 'IssueCommentEvent')  AS comment_count,
        COUNT(DISTINCT actor_login)                                AS unique_contributors
    FROM read_parquet(?)
    GROUP BY 1, 2
) TO ? (FORMAT PARQUET)
"""


def build_serving(src_glob: str, dst: str) -> int:
    con = duckdb.connect()
    try:
        con.execute(ROLLUP_SQL, [dst, src_glob])
    except duckdb.IOException:
        return 0
    return con.execute("SELECT COUNT(*) FROM read_parquet(?)", [dst]).fetchone()[0]
