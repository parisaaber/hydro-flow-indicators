from __future__ import annotations
import os
import boto3
from urllib.parse import urlsplit
from fastapi.responses import StreamingResponse, Response, RedirectResponse

CHUNK = 1024 * 1024  # 1 MiB
S3 = boto3.client("s3")


def _is_remote(path: str) -> bool:
    p = path.lower()
    return p.startswith(("http://", "https://", "s3://"))


def _is_gz(path: str) -> bool:
    return path.lower().endswith(".gz")


def _split_url_like(url: str):
    p = urlsplit(url)
    return p.scheme.lower(), p.netloc, p.path.lstrip("/"), p


def _stream_local(path: str):
    f = open(path, "rb")
    try:
        while True:
            chunk = f.read(CHUNK)
            if not chunk:
                break
            yield chunk
    finally:
        f.close()


def _s3_presign(url: str, *, expires=3600):
    scheme, bucket, key, _ = _split_url_like(url)
    assert scheme == "s3" and bucket
    return S3.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=expires
    )


def map_features(geojson_src: str):
    """
    Local: stream from disk
    Cloud: presign URL for S3, redirect for HTTP(S)
    """
    # Remote sources
    if _is_remote(geojson_src):
        scheme, *_ = _split_url_like(geojson_src)
        if scheme == "s3":
            url = _s3_presign(geojson_src)
            return RedirectResponse(url, status_code=307)
        if scheme in ("http", "https"):
            return RedirectResponse(geojson_src, status_code=307)
        return Response(status_code=400, content=f"Unsupported scheme: {scheme}")

    # Local file
    try:
        os.stat(geojson_src)  # quick existence check
    except FileNotFoundError:
        return Response(status_code=404, content="File not found")

    headers = {"Content-Encoding": "gzip"} if _is_gz(geojson_src) else {}
    media_type = "application/geo+json"
    return StreamingResponse(
        _stream_local(geojson_src), media_type=media_type, headers=headers
    )
