from __future__ import annotations
import requests
import boto3
from urllib.parse import urlsplit
from fastapi.responses import StreamingResponse, Response

CHUNK = 1024 * 1024  # 1 MiB


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


def _stream_http(url: str):
    with requests.get(url, stream=True, timeout=30) as r:
        r.raise_for_status()
        for chunk in r.iter_content(chunk_size=CHUNK):
            if chunk:
                yield chunk


def _stream_s3(url: str):
    # url like s3://bucket/key
    _, bucket, key, _ = _split_url_like(url)
    obj = boto3.client("s3").get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    for chunk in body.iter_chunks(CHUNK):
        if chunk:
            yield chunk


def map_features(
    geojson_src: str,
):
    if not _is_remote(geojson_src):
        iterator = _stream_local(geojson_src)
    else:
        scheme, *_ = _split_url_like(geojson_src)
        if scheme == "s3":
            iterator = _stream_s3(geojson_src)
        elif scheme in ("http", "https"):
            iterator = _stream_http(geojson_src)
        else:
            return Response(status_code=400, content=f"Unsupported scheme: {scheme}")

    headers = {}
    media_type = "application/json"
    if _is_gz(geojson_src):
        headers["Content-Encoding"] = "gzip"

    return StreamingResponse(iterator, media_type=media_type, headers=headers)
