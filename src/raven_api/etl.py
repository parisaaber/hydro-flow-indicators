import os
from tempfile import TemporaryDirectory, _get_candidate_names
from urllib.parse import urlsplit, urlunsplit
import requests
import pandas as pd
import duckdb
import numpy as np
import geopandas as gpd


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
        # Check if the input csv is remote or local
        if not os.path.exists(csv_src):
            csv_dst = os.path.join(tmp_dir, f"{next(_get_candidate_names())}.csv")
            collect_remote(csv_src, csv_dst)
            csv_src = csv_dst

        # Prepare data
        df = load_raven_output(csv_src)
        long_df = reshape_to_long(df)

        # Check if the output is remote or local
        output_is_remote = output_path.lower().startswith("http")
        parquet_dst = (
            os.path.join(tmp_dir, f"{next(_get_candidate_names())}.parquet")
            if output_is_remote
            else output_path
        )

        save_to_parquet(long_df, parquet_dst)

        if output_is_remote:
            upload_to_remote(parquet_dst, output_path)

        if spatial_path and join_column:
            export_spatial_to_geoparquet(
                spatial_path=spatial_path,
                join_column=join_column,
                output_path=output_path,
            )


def collect_remote(url: str, local_path: str) -> None:
    """
    Download a remote file to a local path.

    Args:
        url: URL of the remote CSV file.
        local_path: Local path to save the downloaded file.
    """
    response = requests.get(url)
    response.raise_for_status()

    with open(local_path, "wb") as f:
        f.write(response.content)


def upload_to_remote(local_path: str, remote_url: str) -> None:
    """
    Upload a local file to a remote URL.

    Args:
        local_path: Path to the local file to upload.
        remote_url: URL to upload the file to.
    """
    with open(local_path, "rb") as f:
        response = requests.put(remote_url, data=f)
        response.raise_for_status()


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


def reshape_to_long(df: pd.DataFrame, exclude_precip: bool = True) -> pd.DataFrame:
    """
    Convert wide-format dataframe to long format with optional exclusion of precip columns.

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


def assign_subperiods(df: pd.DataFrame, break_point: int | None) -> pd.DataFrame:
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


def export_spatial_to_geoparquet(
    spatial_path: str,
    join_column: str,
    output_path: str,
) -> str:
    """
    Read a spatial file and write a GeoParquet next to the ETL output.

    The join_column in the written parquet is normalized to 'site'.

    Args:
        spatial_path: Path to the spatial vector file (e.g., .gpkg, .shp, .geojson).
        join_column: Name of the column of the sites for spatial joins.
        output_path: The CSV timeseries output path/URL used to determine sibling location.

    Returns:
        The local full path (for local outputs) or the remote URL (for remote outputs).
    """
    gdf = gpd.read_file(spatial_path)

    if join_column not in gdf.columns:
        raise ValueError(
            f"join_column '{join_column}' not found in spatial data. "
            f"Available columns: {list(gdf.columns)}"
        )

    # Normalize join column name to 'site', used by the CSV timeseries.
    if join_column != "site":
        if "site" in gdf.columns:
            raise ValueError(
                "The spatial data already contains a 'site' column, but you also "
                f"specified join_column='{join_column}'. Please resolve the conflict."
            )
        gdf = gdf.rename(columns={join_column: "site"})

    base = os.path.splitext(os.path.basename(spatial_path))[0]
    out_name = f"{base}.parquet"
    output_is_remote = output_path.lower().startswith(("http://", "https://"))

    if not output_is_remote:
        out_dir = os.path.dirname(os.path.abspath(output_path))
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, out_name)
        gdf.to_parquet(out_path, index=False)
        return out_path

    # Replace the last segment of the output_path with out_name
    parts = urlsplit(output_path)
    path_segments = parts.path.rstrip("/").split("/")
    if path_segments:
        path_segments[-1] = out_name
    new_path = "/".join(path_segments)
    remote_url = urlunsplit(
        (parts.scheme, parts.netloc, new_path, parts.query, parts.fragment)
    )

    with TemporaryDirectory() as tmp_dir:
        tmp_parquet = os.path.join(tmp_dir, out_name)
        gdf.to_parquet(tmp_parquet, index=False)
        upload_to_remote(tmp_parquet, remote_url)
        return remote_url
