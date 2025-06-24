import os
from tempfile import TemporaryDirectory, _get_candidate_names

import requests
import pandas as pd
import duckdb
import numpy as np


def init_etl(csv_src: str, output_path: str) -> None:
    """
    Initialize the ETL process for a Raven output CSV file.

    Args:
        csv_src: Path to the input CSV file.
        output_path: Path to save the processed Parquet file.
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
    print(f"✅ Saved to {out_path}")


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
