from fastapi import FastAPI
from typing import Optional, List
import pandas as pd
import numpy as np
import duckdb
from scipy.stats import gumbel_r
import tempfile
import os


app = FastAPI(title="Raven API", version="0.1")


def mean_annual_flow(con: duckdb.DuckDBPyConnection, parquet_path: str, sites: Optional[List[str]] = None) -> pd.DataFrame:
    if sites:
        sites_tuple = tuple(sites)
        query = f"""
            WITH mean_1 AS (
                SELECT avg(value) AS maf_per_year, site, water_year
                FROM parquet_scan('{parquet_path}')
                WHERE site IN {sites_tuple}
                GROUP BY site, water_year
            )
            SELECT site, avg(maf_per_year) AS mean_annual_flow
            FROM mean_1
            GROUP BY site
            ORDER BY site
        """
    else:
        query = f"""
            WITH mean_1 AS (
                SELECT avg(value) AS maf_per_year, site, water_year
                FROM parquet_scan('{parquet_path}')
                GROUP BY site, water_year
            )
            SELECT site, avg(maf_per_year) AS mean_annual_flow
            FROM mean_1
            GROUP BY site
            ORDER BY site
        """
    return con.execute(query).fetchdf()


def mean_annual_flow(con: duckdb.DuckDBPyConnection, parquet_path: str) -> pd.DataFrame:
    """
    Calculate the mean annual flow (MAF) for each site.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site' and 'mean_annual_flow' columns.
    """
    return con.execute(
        f"""
        WITH mean_1 AS (
            SELECT avg(value) AS maf_per_year, site, water_year
            FROM parquet_scan('{parquet_path}')
            GROUP BY site, water_year
        )
        SELECT site, avg(maf_per_year) AS mean_annual_flow
        FROM mean_1
        GROUP BY site
        ORDER BY site
        """
    ).fetchdf()


def mean_aug_sep_flow(con: duckdb.DuckDBPyConnection, parquet_path: str) -> pd.DataFrame:
    """
    Calculate the mean August–September flow for each site.


    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

def mean_aug_sep_flow(con: duckdb.DuckDBPyConnection, parquet_path: str, sites: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Calculate the mean August–September flow for each site.
=======
    Returns:
        DataFrame with 'site' and 'mean_aug_sep_flow' columns.
    """
    return con.execute(
        f"""
        WITH aug_sep AS (
            SELECT site, water_year, AVG(value) AS avg_aug_sep_flow
            FROM parquet_scan('{parquet_path}')
            WHERE EXTRACT(month FROM date) IN (8, 9)
            GROUP BY site, water_year
        )
        SELECT site, AVG(avg_aug_sep_flow) AS mean_aug_sep_flow
        FROM aug_sep
        GROUP BY site
        ORDER BY site
        """
    ).fetchdf()

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site' and 'mean_aug_sep_flow' columns.
    """
    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND site IN {sites_tuple}"
    query = f"""
        WITH aug_sep AS (
            SELECT site, water_year, AVG(value) AS avg_aug_sep_flow
            FROM parquet_scan('{parquet_path}')
            WHERE EXTRACT(month FROM date) IN (8, 9)
            {site_filter}
            GROUP BY site, water_year
        )
        SELECT site, AVG(avg_aug_sep_flow) AS mean_aug_sep_flow
        FROM aug_sep
        GROUP BY site
        ORDER BY site
        """
    return con.execute(query).fetchdf()


def peak_flow_timing(con: duckdb.DuckDBPyConnection, parquet_path: str, sites: Optional[List[str]] = None) -> pd.DataFrame:
    """
=======
def peak_flow_timing(con: duckdb.DuckDBPyConnection, parquet_path: str) -> pd.DataFrame:
    """
    Estimate the average day of year of annual peak flow for each site.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site' and 'peak_flow_timing' columns.
    """

    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND site IN {sites_tuple}"
    query= f"""
        WITH daily AS (
            SELECT site, water_year, value, EXTRACT(doy FROM date) AS doy
            FROM parquet_scan('{parquet_path}')
            WHERE value IS NOT NULL
            {site_filter}
=======
    return con.execute(
        f"""
        WITH daily AS (
            SELECT site, water_year, value, EXTRACT(doy FROM date) AS doy
            FROM parquet_scan('{parquet_path}')

        ),
        peaks AS (
            SELECT site, water_year,
                   FIRST_VALUE(doy) OVER (PARTITION BY site, water_year ORDER BY value DESC) AS peak_doy
            FROM daily
        )
        SELECT site, AVG(peak_doy) AS peak_flow_timing
        FROM peaks
        GROUP BY site
        ORDER BY site
        """

    return con.execute(query).fetchdf()


def days_below_efn(con: duckdb.DuckDBPyConnection, parquet_path: str, EFN_threshold: float, sites: Optional[List[str]] = None) -> pd.DataFrame:
=======
    ).fetchdf()


def days_below_efn(con: duckdb.DuckDBPyConnection, parquet_path: str, EFN_threshold: float) -> pd.DataFrame:

    """
    Calculate the average number of days per year below the Environmental Flow Needs (EFN) threshold.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.
        EFN_threshold: Proportion (e.g., 0.2) of mean annual flow used as the threshold.

    Returns:
        DataFrame with 'site' and 'days_below_efn' columns.
    """

    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND p.site IN {sites_tuple}"
    query= f"""
=======
    return con.execute(
        f"""

        WITH mean_annual AS (
            SELECT site, water_year, AVG(value) AS maf_year
            FROM parquet_scan('{parquet_path}')
            WHERE value IS NOT NULL
            GROUP BY site, water_year
        ),
        mean_annual_site AS (
            SELECT site, AVG(maf_year) AS maf_site
            FROM mean_annual
            GROUP BY site
        ),
        daily_with_threshold AS (
            SELECT p.site, p.water_year, p.value,
                   m.maf_site, m.maf_site * {EFN_threshold} AS threshold
            FROM parquet_scan('{parquet_path}') p
            JOIN mean_annual_site m ON p.site = m.site

            WHERE value IS NOT NULL
            {site_filter}
=======
            WHERE p.value IS NOT NULL

        ),
        days_below AS (
            SELECT site, water_year,
                   COUNT(*) FILTER (WHERE value < threshold) AS days_below_efn
            FROM daily_with_threshold
            GROUP BY site, water_year
        )
        SELECT site, AVG(days_below_efn) AS days_below_efn
        FROM days_below
        GROUP BY site
        ORDER BY site
        """

    return con.execute(query).fetchdf()


def annual_peaks(con: duckdb.DuckDBPyConnection, parquet_path: str, sites: Optional[List[str]] = None) -> pd.DataFrame:
=======
    ).fetchdf()


def annual_peaks(con: duckdb.DuckDBPyConnection, parquet_path: str) -> pd.DataFrame:

    """
    Extract annual peak flow for each site and water year.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site', 'water_year', and 'annual_peak' columns.
    """

    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND site IN {sites_tuple}"
    query= f"""
        SELECT site, water_year, MAX(value) AS annual_peak
        FROM parquet_scan('{parquet_path}')
        WHERE Value IS NOT NULL
        {site_filter}
        GROUP BY site, water_year
        ORDER BY site, water_year
        """
    return con.execute(query).fetchdf()


def fit_ffa(peaks_df: pd.DataFrame, dist: str = "gumbel", return_periods: list[int] = [2, 20], sites: Optional[List[str]] = None) -> pd.DataFrame:
=======
    return con.execute(
        f"""
        SELECT site, water_year, MAX(value) AS annual_peak
        FROM parquet_scan('{parquet_path}')
        GROUP BY site, water_year
        ORDER BY site, water_year
        """
    ).fetchdf()


def fit_ffa(peaks_df: pd.DataFrame, dist: str = "gumbel", return_periods: list[int] = [2, 20]) -> pd.DataFrame:

    """
    Fit Flood Frequency Analysis using Gumbel distribution for specified return periods.

    Args:
        peaks_df: DataFrame with 'site', 'water_year', 'annual_peak'.
        dist: Distribution name (currently only 'gumbel' is supported).
        return_periods: List of return periods to estimate (e.g., [2, 20]).

    Returns:
        DataFrame with 'site' and return period discharge values (e.g., 'Q2', 'Q20').
    """
    if sites:
        peaks_df = peaks_df[peaks_df["site"].isin(sites)]
    result = []
    for site, group in peaks_df.groupby("site"):
        values = group["annual_peak"].dropna()
        if len(values) < 2:
            continue
        if dist == "gumbel":
            loc, scale = gumbel_r.fit(values)
            rp_values = {f"Q{rp}": gumbel_r.ppf(1 - 1 / rp, loc=loc, scale=scale) for rp in return_periods}
            rp_values["site"] = site
            result.append(rp_values)
    return pd.DataFrame(result)



def peak_flows(con: duckdb.DuckDBPyConnection, parquet_path: str, sites: Optional[List[str]] = None) -> pd.DataFrame:
=======
def peak_flows(con: duckdb.DuckDBPyConnection, parquet_path: str) -> pd.DataFrame:

    """
    Calculate mean annual peak flow for each site.

    Args:
        con: Active DuckDB connection.
        parquet_path: Path to input Parquet file.

    Returns:
        DataFrame with 'site' and 'mean_annual_peak' columns.
    """

    site_filter = ""
    if sites:
        sites_tuple = tuple(sites)
        site_filter = f"AND site IN {sites_tuple}"
    query= f"""
        WITH annual_peaks AS (
            SELECT site, water_year, MAX(value) AS annual_peak
            FROM parquet_scan('{parquet_path}')
            WHERE value IS NOT NULL
            {site_filter}
=======
    return con.execute(
        f"""
        WITH annual_peaks AS (
            SELECT site, water_year, MAX(value) AS annual_peak
            FROM parquet_scan('{parquet_path}')

            GROUP BY site, water_year
        )
        SELECT site, AVG(annual_peak) AS mean_annual_peak
        FROM annual_peaks
        GROUP BY site
        ORDER BY site
        """

    return con.execute(query).fetchdf()
=======
    ).fetchdf()



def calculate_all_indicators(
    parquet_path: str, EFN_threshold: float = 0.2, break_point: Optional[int] = None,sites: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Calculate a set of hydrologic indicators for one or more subperiods.

    Indicators include:
    - Mean annual flow
    - August–September mean flow
    - Peak flow timing (DOY)
    - Number of days below EFN threshold
    - Mean annual peak flow
    - Flood quantiles (Q2, Q20)

    Args:
        parquet_path: Path to Raven-generated Parquet file.
        EFN_threshold: Proportion of MAF used for low flow threshold (e.g., 0.2).
        break_point: Water year to split subperiods (e.g., 2000), or None for full period.

    Returns:
        DataFrame containing calculated indicators for each site and subperiod.
    """
    try:
        con = duckdb.connect()
        df = con.execute(
            f"""
            SELECT date, value, site
            FROM parquet_scan('{parquet_path}')
            WHERE value IS NOT NULL
            """
        ).fetchdf()
    except Exception as e:
        raise RuntimeError(f"Failed to read parquet file: {e}")

    try:
        df["date"] = pd.to_datetime(df["date"])
        df["month"] = df["date"].dt.month
        df["year"] = df["date"].dt.year
        df["water_year"] = np.where(df["month"] >= 10, df["year"] + 1, df["year"])
        df["subperiod"] = (
            np.where(df["water_year"] <= break_point, f"before_{break_point}", f"after_{break_point}")
            if break_point else "full_period"
        )

        results = []

        for period in df["subperiod"].unique():
            sub_df = df[df["subperiod"] == period]
            with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmpfile:
                temp_parquet_path = tmpfile.name
            sub_df.to_parquet(temp_parquet_path)

            indicators = pd.concat(
                [
                    mean_annual_flow(con, temp_parquet_path).set_index("site"),
                    mean_aug_sep_flow(con, temp_parquet_path).set_index("site"),
                    peak_flow_timing(con, temp_parquet_path).set_index("site"),
                    days_below_efn(con, temp_parquet_path, EFN_threshold).set_index("site"),
                    peak_flows(con, temp_parquet_path).set_index("site"),
                ],
                axis=1,
            )

            peaks_df = annual_peaks(con, temp_parquet_path)
            ffa = fit_ffa(peaks_df, return_periods=[2, 20]).set_index("site")

            all_indicators = indicators.join(ffa, how="left")
            all_indicators["subperiod"] = period
            results.append(all_indicators.reset_index())

            os.remove(temp_parquet_path)

        con.close()
        final_df = pd.concat(results, ignore_index=True)
        final_df.replace([np.nan, np.inf, -np.inf], None, inplace=True)
        return final_df

    except Exception as e:
        raise RuntimeError(f"Error in calculating indicators: {e}")
