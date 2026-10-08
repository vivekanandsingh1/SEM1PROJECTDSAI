from datetime import datetime

from ghpulse import aggregate, config, extract, load, transform


def run_hour(ts: datetime) -> dict:
    raw_dest = config.raw_path(ts, ts.hour) / "events.json.gz"
    curated_dest = config.curated_path(ts, ts.hour) / "events.parquet"
    serving_dest = config.serving_path()
    for p in (raw_dest, curated_dest, serving_dest):
        p.parent.mkdir(parents=True, exist_ok=True)

    path, bytes_raw = extract.download_hour(ts, raw_dest)
    total, written = transform.to_parquet(str(path), str(curated_dest))

    day_glob = str(config.curated_path(ts, ts.hour).parent / "*" / "events.parquet")
    serving_rows = aggregate.build_serving(day_glob, str(serving_dest))

    rows_loaded = load.publish_to_postgres(str(serving_dest), config.warehouse_url())

    return {
        "ts": ts,
        "bytes_raw": bytes_raw,
        "rows_written": written,
        "serving_rows": serving_rows,
        "rows_loaded": rows_loaded,
    }


def run_day(date: datetime) -> dict:
    results: list[dict] = []
    errors: list[dict] = []
    for h in range(24):
        ts = date.replace(hour=h, minute=0, second=0, microsecond=0)
        try:
            results.append(run_hour(ts))
        except Exception as e:
            errors.append({"hour": h, "error": type(e).__name__, "detail": str(e)})
    return {
        "date": date.date(),
        "hours_ok": len(results),
        "errors": errors,
        "rows_written": sum(r["rows_written"] for r in results),
        "rows_loaded": results[-1]["rows_loaded"] if results else 0,
    }
