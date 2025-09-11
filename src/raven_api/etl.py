import os
import re
from tempfile import TemporaryDirectory, _get_candidate_names
from urllib.parse import urlsplit, urlunsplit
import requests
import boto3
import pandas as pd
import duckdb
import numpy as np
import geopandas as gpd
import io, gzip
from pyproj import CRS

_S3_HTTPS_URL_REGEX = re.compile(
    r"^(?P<bucket>[^.]+)\.(?:s3(?:[.-][a-z0-9-]+)?|s3-accelerate)\.amazonaws\.com$"
)


def _split_url(url: str):
    p = urlsplit(url)
    return p.scheme.lower(), p.netloc, p.path.lstrip("/"), p


def _is_remote(url_or_path: str) -> bool:
    scheme, *_ = _split_url(url_or_path)
    return scheme in ("http", "https", "s3")


def _normalize_s3_url(url: str) -> str | None:
    """
    Normalize S3 URLs (s3:// or S3 HTTPS) into s3://bucket/key form.
    Returns None if not S3.
    """
    p = urlsplit(url)
    scheme = p.scheme.lower()

    # s3://bucket/key
    if scheme == "s3":
        return url

    # S3-style HTTPS
    parsed = _try_parse_s3_https(url)
    if parsed:
        bucket, key = parsed
        return f"s3://{bucket}/{key}"

    return None


def _try_parse_s3_https(url: str):
    """Return (bucket, key) if url is an S3 HTTPS object URL, else None."""
    p = urlsplit(url)
    if p.scheme.lower() != "https":
        return None
    m = _S3_HTTPS_URL_REGEX.match(p.netloc)
    if m:
        # path-style: https://{bucket}.s3.{region}.amazonaws.com/{key}
        return m.group("bucket"), p.path.lstrip("/")
    # path-style: https://s3.{region}.amazonaws.com/{bucket}/{key}
    if p.netloc.startswith("s3.") and p.netloc.endswith(".amazonaws.com"):
        path = p.path.lstrip("/")
        if "/" in path:
            bucket, key = path.split("/", 1)
            return bucket, key
    return None


def init_etl(
    csv_src: str,
    output_path: str,
    spatial_path: str | None = None,
    join_column: str | None = None,
) -> None:
    """
    Initialize the ETL process for a Raven output CSV file, and optionally
    export a spatial file to GeoParquet for later joins.

    Args:
        csv_src: Path/URL to the input CSV file.
        output_path: Path/URL to save the processed Parquet file.
        spatial_path: Optional path to a spatial vector file (e.g., GeoPackage, Shapefile, GeoJSON).
        join_column: Optional name of the column referring to the sites for spatial joins.
    """
    with TemporaryDirectory() as tmp_dir:
        if not os.path.exists(csv_src):
            csv_dst = os.path.join(
                tmp_dir, f"{next(_get_candidate_names())}.csv"
                )
            collect_remote(csv_src, csv_dst)
            csv_src = csv_dst

        df = load_raven_output(csv_src)
        long_df = reshape_to_long(df)

        output_is_remote = _is_remote(output_path)
        parquet_dst = (
            os.path.join(tmp_dir, f"{next(_get_candidate_names())}.parquet")
            if output_is_remote
            else output_path
        )

        save_to_parquet(long_df, parquet_dst)

        if output_is_remote:
            upload_to_remote(parquet_dst, output_path)

        if spatial_path and join_column:
            expected_sites = long_df["site"].dropna().astype(str).unique().tolist()
            export_spatial_to_geojson_gz(
                spatial_path=spatial_path,
                join_column=join_column,
                output_path=output_path,
                expected_sites=expected_sites,
            )


def collect_remote(url: str, local_path: str) -> None:
    """
    Download a remote file to a local path.

    Args:
        url: URL of the remote CSV file.
        local_path: Local path to save the downloaded file.
    """
    scheme, netloc, path, _ = _split_url(url)
    if scheme == "s3":
        boto3.client("s3").download_file(netloc, path, local_path)
        return
    r = requests.get(url)
    r.raise_for_status()
    with open(local_path, "wb") as f:
        f.write(r.content)


def upload_to_remote(local_path: str, remote_url: str) -> None:
    # normalize to s3:// if possible
    s3_url = _normalize_s3_url(remote_url)
    if s3_url:
        scheme, bucket, key, _ = _split_url(s3_url)
        boto3.client("s3").upload_file(local_path, bucket, key)
        return

    # otherwise assume presigned PUT (true generic HTTPS)
    scheme, *_ = _split_url(remote_url)
    if scheme in ("http", "https"):
        with open(local_path, "rb") as f:
            r = requests.put(remote_url, data=f)
            r.raise_for_status()
        return

    raise ValueError(f"Unsupported remote scheme '{scheme}' for {remote_url}")


def _sibling_remote(output_path: str, out_name: str) -> str:
    """
    Return sibling path next to output_path.
    Normalizes S3-style URLs → s3://bucket/key.
    """
    s3_url = _normalize_s3_url(output_path)
    if s3_url:
        scheme, bucket, key, _ = _split_url(s3_url)
        segs = key.rstrip("/").split("/")
        if segs:
            segs[-1] = out_name
        return f"s3://{bucket}/" + "/".join(segs)

    # fallback for true generic http(s)
    p = urlsplit(output_path)
    segs = p.path.rstrip("/").split("/")
    if segs:
        segs[-1] = out_name
    return urlunsplit((p.scheme, p.netloc, "/".join(segs), p.query, p.fragment))


def load_raven_output(csv_path: str) -> pd.DataFrame:
    """
    Load Raven output CSV, clean and structure it.

    Args:
        csv_path: Path to the CSV file.

    Returns:
        A DataFrame indexed by date with model output columns.
    """
    df = pd.read_csv(csv_path, sep="\t|,", engine="python")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    df = df.drop(columns=["time", "hour"], errors="ignore")
    df = df.set_index("date")
    df = df.loc[:, ~df.columns.str.contains("observed", case=False)]

    return df


def reshape_to_long(
    df: pd.DataFrame, exclude_precip: bool = True
) -> pd.DataFrame:
    """
    Convert wide-format dataframe to long format
    with optional exclusion of precip columns.

    Args:
        df: DataFrame in wide format.
        exclude_precip: Whether to remove precip columns.

    Returns:
        Long-format DataFrame with columns: date, site, value.
    """
    df = df.reset_index()
    if exclude_precip:
        precip_cols = [col for col in df.columns if "precip" in col.lower()]
        df = df.drop(columns=precip_cols, errors="ignore")
    long_df = df.melt(id_vars="date", var_name="site", value_name="value")

    # Convert date to datetime and extract month, year, and water year
    long_df["month"] = long_df["date"].dt.month
    long_df["year"] = long_df["date"].dt.year
    long_df["water_year"] = np.where(
        long_df["month"] >= 10, long_df["year"] + 1, long_df["year"]
    )

    long_df["value"] = pd.to_numeric(long_df["value"], errors="coerce")
    return long_df.dropna(subset=["value"])


def save_to_parquet(df: pd.DataFrame, out_path: str) -> None:
    """
    Save DataFrame to a Parquet file.

    Args:
        df: DataFrame to save.
        out_path: Output path for Parquet file.
    """
    df.to_parquet(out_path)


def load_parquet_data(parquet_path: str) -> pd.DataFrame:
    """
    Load Parquet data using DuckDB, adding month, year, and water year.

    Args:
        parquet_path: Path to Parquet file.

    Returns:
        Processed DataFrame with date, site, value, and time columns.
    """
    con = duckdb.connect()
    query = f"""
        SELECT date, value, site
        FROM parquet_scan('{parquet_path}')
        WHERE value IS NOT NULL
    """
    df = con.execute(query).fetchdf()
    df["date"] = pd.to_datetime(df["date"])
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["water_year"] = np.where(df["month"] >= 10, df["year"] + 1, df["year"])
    return df


def assign_subperiods(
    df: pd.DataFrame, break_point: int | None
) -> pd.DataFrame:
    """
    Add a 'subperiod' column to the DataFrame based on water year break.

    Args:
        df: DataFrame with a 'water_year' column.
        break_point: Year to split data into 'before' and 'after' subperiods.

    Returns:
        DataFrame with a new 'subperiod' column.
    """
    if break_point is not None:
        df["subperiod"] = np.where(
            df["water_year"] <= break_point,
            f"before_{break_point}",
            f"after_{break_point}",
        )
    else:
        df["subperiod"] = "full_period"
    return df


def export_spatial_to_geojson_gz(
    spatial_path: str,
    join_column: str,
    output_path: str,
    expected_sites: list[str] | None = None,
) -> str:
    """
    Read a spatial file and write a gzipped GeoJSON next to the ETL output.

    The join_column in the written file is normalized to 'site'.
    Returns the local path (for local outputs) or the remote URL (for remote outputs).
    """

    gdf = gpd.read_file(spatial_path)

    if join_column not in gdf.columns:
        raise ValueError(
            f"join_column '{join_column}' not found in spatial data. "
            f"Available columns: {list(gdf.columns)}"
        )

    if join_column != "site":
        if "site" in gdf.columns:
            raise ValueError(
                "The spatial data already contains a 'site' column, but you also "
                f"specified join_column='{join_column}'. Please resolve the conflict."
            )
        gdf = gdf.rename(columns={join_column: "site"})

    if expected_sites is not None:
        spatial_sites = set(gdf["site"].dropna().astype(str))
        expected_sites_set = set(map(str, expected_sites))
        if not (spatial_sites & expected_sites_set):
            raise ValueError(
                "No matching site values found between spatial data and timeseries. "
                f"Example spatial sites: {list(spatial_sites)[:10]} ; "
                f"example expected sites: {list(expected_sites_set)[:10]}"
            )

    if gdf.crs is not None:
        try:
            if not CRS.from_user_input(gdf.crs).equals(CRS.from_epsg(4326)):
                gdf = gdf.to_crs(4326)
        except Exception:
            pass

    geojson_text = gdf.to_json(drop_id=True)
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(geojson_text.encode("utf-8"))
    gz_bytes = buf.getvalue()

    base = os.path.splitext(os.path.basename(spatial_path))[0]
    out_name = f"{base}.geojson.gz"

    if not _is_remote(output_path):
        out_dir = os.path.dirname(os.path.abspath(output_path))
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, out_name)
        with open(out_path, "wb") as f:
            f.write(gz_bytes)
        return out_path

    # remote sibling
    remote_url = _sibling_remote(output_path, out_name)

    # If S3, set the metadata so browsers auto-decompress
    s3_url = _normalize_s3_url(remote_url)
    if s3_url:
        _, bucket, key, _ = _split_url(s3_url)
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=key,
            Body=gz_bytes,
            ContentType="application/json",
            ContentEncoding="gzip",
        )
        return remote_url

    # Generic HTTPS PUT (e.g., presigned)
    scheme, *_ = _split_url(remote_url)
    if scheme in ("http", "https"):
        r = requests.put(
            remote_url,
            data=gz_bytes,
            headers={
                "Content-Type": "application/json",
                "Content-Encoding": "gzip",
            },
        )
        r.raise_for_status()
        return remote_url

    raise ValueError(f"Unsupported remote scheme for {remote_url}")
