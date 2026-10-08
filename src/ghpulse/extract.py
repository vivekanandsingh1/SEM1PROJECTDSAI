import gzip
from datetime import datetime
from pathlib import Path

import httpx
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential

from ghpulse.config import GHARCHIVE_BASE_URL


class NotPublishedYet(Exception):
    pass


class TruncatedDownload(Exception):
    pass


def _should_retry(exc: BaseException) -> bool:
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code == 429 or exc.response.status_code >= 500
    return isinstance(exc, (httpx.TransportError, NotPublishedYet, TruncatedDownload))


def _assert_valid_gzip(path: Path) -> None:
    if path.stat().st_size == 0:
        raise TruncatedDownload(f"Zero-byte file: {path}")
    decompressed = 0
    try:
        with gzip.open(path, "rb") as f:
            while chunk := f.read(1024 * 1024):
                decompressed += len(chunk)
    except EOFError:
        raise TruncatedDownload(f"Truncated gzip stream: {path}")
    if decompressed == 0:
        raise TruncatedDownload(f"Empty gzip stream: {path}")


@retry(
    retry=retry_if_exception(_should_retry),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    stop=stop_after_attempt(5),
    reraise=True,
)
def download_hour(ts: datetime, dest: Path) -> tuple[Path, int]:
    url = f"{GHARCHIVE_BASE_URL}/{ts.strftime('%Y-%m-%d')}-{ts.hour}.json.gz"
    tmp = dest.with_suffix(".tmp")
    dest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with httpx.stream("GET", url) as r:
            if r.status_code == 404:
                raise NotPublishedYet(url)
            r.raise_for_status()
            bytes_written = 0
            with tmp.open("wb") as fh:
                for chunk in r.iter_raw(chunk_size=65536):
                    fh.write(chunk)
                    bytes_written += len(chunk)
            cl = r.headers.get("content-length")
            if cl is not None and int(cl) != bytes_written:
                raise TruncatedDownload(
                    f"Content-Length {cl} != bytes written {bytes_written}"
                )
        _assert_valid_gzip(tmp)
        tmp.replace(dest)
        return dest, bytes_written
    except Exception:
        tmp.unlink(missing_ok=True)
        raise
