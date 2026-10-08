import gzip
import io
from datetime import datetime
from unittest.mock import MagicMock, patch

import httpx
import pytest

from ghpulse.extract import (
    NotPublishedYet,
    TruncatedDownload,
    _assert_valid_gzip,
    _should_retry,
    download_hour,
)


def _make_gzip(content: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as f:
        f.write(content)
    return buf.getvalue()


def _make_empty_gzip() -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as _:
        pass
    return buf.getvalue()


def _mock_stream(status_code=200, body=b"", headers=None, read_error=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.headers = headers if headers is not None else {}
    if status_code >= 400 and status_code != 404:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    if read_error is not None:
        def _error_iter(chunk_size):
            yield body
            raise read_error
        resp.iter_raw.side_effect = _error_iter
    else:
        resp.iter_raw.side_effect = lambda chunk_size: iter([body] if body else [])
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=resp)
    cm.__exit__ = MagicMock(return_value=False)
    return cm



def test_should_retry_transport_error():
    assert _should_retry(httpx.TransportError("conn reset")) is True


def test_should_retry_not_published_yet():
    assert _should_retry(NotPublishedYet()) is True


def test_should_retry_truncated_download():
    assert _should_retry(TruncatedDownload()) is True


def test_should_retry_500():
    resp = MagicMock()
    resp.status_code = 500
    assert _should_retry(httpx.HTTPStatusError("", request=MagicMock(), response=resp)) is True


def test_should_not_retry_bad_gzip_file():
    assert _should_retry(gzip.BadGzipFile()) is False


def test_should_not_retry_403():
    resp = MagicMock()
    resp.status_code = 403
    assert _should_retry(httpx.HTTPStatusError("", request=MagicMock(), response=resp)) is False



def test_assert_valid_gzip_passes_on_valid_file(tmp_path):
    p = tmp_path / "ok.gz"
    p.write_bytes(_make_gzip(b'{"type":"PushEvent"}'))
    _assert_valid_gzip(p)


def test_assert_valid_gzip_zero_byte_file_raises(tmp_path):
    p = tmp_path / "empty.gz"
    p.write_bytes(b"")
    with pytest.raises(TruncatedDownload):
        _assert_valid_gzip(p)


def test_assert_valid_gzip_empty_gzip_stream_raises(tmp_path):
    p = tmp_path / "empty_stream.gz"
    p.write_bytes(_make_empty_gzip())
    with pytest.raises(TruncatedDownload):
        _assert_valid_gzip(p)


def test_assert_valid_gzip_corrupt_raises_bad_gzip_file(tmp_path):
    p = tmp_path / "corrupt.gz"
    p.write_bytes(b"\x00\x01\x02not a gzip file")
    with pytest.raises(gzip.BadGzipFile):
        _assert_valid_gzip(p)


def test_assert_valid_gzip_truncated_raises_truncated_download(tmp_path):
    p = tmp_path / "truncated.gz"
    full = _make_gzip(b"hello world this is some content")
    p.write_bytes(full[:8])
    with pytest.raises(TruncatedDownload):
        _assert_valid_gzip(p)



VALID_BODY = _make_gzip(b'{"type":"PushEvent","id":"1"}')


def test_source_url_not_zero_padded(tmp_path, monkeypatch):
    dest = tmp_path / "out.json.gz"
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(body=VALID_BODY)
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        download_hour(datetime(2024, 1, 15, 7), dest)
        url = mock_stream.call_args[0][1]
    assert url.endswith("-7.json.gz"), f"Expected -7.json.gz, got: {url}"


def test_happy_path(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(body=VALID_BODY)
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        path, n = download_hour(datetime(2024, 1, 15, 14), dest)
    assert path == dest
    assert dest.exists()
    assert n == len(VALID_BODY)


def test_happy_path_creates_parent_dirs(tmp_path, monkeypatch):
    dest = tmp_path / "deep" / "nested" / "events.json.gz"
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(body=VALID_BODY)
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        download_hour(datetime(2024, 1, 15, 14), dest)
    assert dest.exists()


def test_idempotent_rerun(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    dest.write_bytes(b"old content")
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(body=VALID_BODY)
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        download_hour(datetime(2024, 1, 15, 14), dest)
    assert dest.read_bytes() == VALID_BODY


def test_corrupt_gzip_raises_and_cleans_tmp(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    corrupt = b"\x00\x01\x02not a gzip"
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(body=corrupt)
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        with pytest.raises(gzip.BadGzipFile):
            download_hour(datetime(2024, 1, 15, 14), dest)
    assert not dest.with_suffix(".tmp").exists()


def test_content_length_mismatch_raises_truncated_download(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(
            body=VALID_BODY,
            headers={"content-length": str(len(VALID_BODY) + 100)},
        )
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        with pytest.raises(TruncatedDownload):
            download_hour(datetime(2024, 1, 15, 14), dest)


def test_content_encoding_does_not_false_fail_content_length(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(
            body=VALID_BODY,
            headers={
                "content-length": str(len(VALID_BODY)),
                "content-encoding": "gzip",
            },
        )
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        path, n = download_hour(datetime(2024, 1, 15, 14), dest)
    assert dest.exists()



def test_retries_on_500_then_succeeds(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    resp_500 = MagicMock()
    resp_500.status_code = 500
    resp_500.headers = {}
    resp_500.raise_for_status.side_effect = httpx.HTTPStatusError(
        "HTTP 500", request=MagicMock(), response=resp_500
    )
    cm_500 = MagicMock()
    cm_500.__enter__ = MagicMock(return_value=resp_500)
    cm_500.__exit__ = MagicMock(return_value=False)
    cm_200 = _mock_stream(body=VALID_BODY)
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return cm_500 if call_count == 1 else cm_200

    with patch("ghpulse.extract.httpx.stream", side_effect=side_effect):
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        path, _ = download_hour(datetime(2024, 1, 15, 14), dest)
    assert dest.exists()
    assert call_count == 2


def test_retry_exhaustion_5_attempts(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    resp = MagicMock()
    resp.status_code = 500
    resp.headers = {}
    resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "HTTP 500", request=MagicMock(), response=resp
    )
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=resp)
    cm.__exit__ = MagicMock(return_value=False)
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return cm

    with patch("ghpulse.extract.httpx.stream", side_effect=side_effect):
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        with pytest.raises(httpx.HTTPStatusError):
            download_hour(datetime(2024, 1, 15, 14), dest)
    assert call_count == 5


def test_403_not_retried_fails_after_one_attempt(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    resp = MagicMock()
    resp.status_code = 403
    resp.headers = {}
    resp.raise_for_status.side_effect = httpx.HTTPStatusError(
        "HTTP 403", request=MagicMock(), response=resp
    )
    cm = MagicMock()
    cm.__enter__ = MagicMock(return_value=resp)
    cm.__exit__ = MagicMock(return_value=False)
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return cm

    with patch("ghpulse.extract.httpx.stream", side_effect=side_effect):
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        with pytest.raises(httpx.HTTPStatusError):
            download_hour(datetime(2024, 1, 15, 14), dest)
    assert call_count == 1


def test_404_raises_not_published_yet_after_5_attempts(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return _mock_stream(status_code=404)

    with patch("ghpulse.extract.httpx.stream", side_effect=side_effect):
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        with pytest.raises(NotPublishedYet):
            download_hour(datetime(2024, 1, 15, 14), dest)
    assert call_count == 5


def test_read_error_mid_stream_cleans_tmp_after_exhaustion(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(
            body=b"partial", read_error=httpx.TransportError("connection reset")
        )
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        with pytest.raises(httpx.TransportError):
            download_hour(datetime(2024, 1, 15, 14), dest)
    assert not dest.with_suffix(".tmp").exists()


def test_truncated_gzip_retried_then_raises(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    truncated = _make_gzip(b"hello world this is content")[:8]
    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        return _mock_stream(body=truncated)

    with patch("ghpulse.extract.httpx.stream", side_effect=side_effect):
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        with pytest.raises(TruncatedDownload):
            download_hour(datetime(2024, 1, 15, 14), dest)
    assert call_count == 5


def test_zero_byte_body_raises_truncated_download(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(body=b"")
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        with pytest.raises(TruncatedDownload):
            download_hour(datetime(2024, 1, 15, 14), dest)


def test_empty_gzip_stream_raises_truncated_download(tmp_path, monkeypatch):
    dest = tmp_path / "events.json.gz"
    with patch("ghpulse.extract.httpx.stream") as mock_stream:
        mock_stream.return_value = _mock_stream(body=_make_empty_gzip())
        monkeypatch.setattr(download_hour.retry, "sleep", lambda s: None)
        with pytest.raises(TruncatedDownload):
            download_hour(datetime(2024, 1, 15, 14), dest)
